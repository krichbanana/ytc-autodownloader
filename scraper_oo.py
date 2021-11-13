#!/usr/bin/env python3
import os
import subprocess
import sys
import json
import multiprocessing as mp
import datetime as dt
import time
import traceback
import signal
import typing
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Optional,
    Set
)

from bs4 import BeautifulSoup  # type: ignore
from chat_downloader import ChatDownloader  # type: ignore
from chat_downloader.sites import YouTubeChatDownloader  # type: ignore

from utils import (
    check_pid,
    extract_video_id_from_yturl,
    json_stream_wrapper,
    meta_load_fast,
    meta_extract_start_timestamp,
    meta_extract_raw_live_status
)


try:
    from write_cgroup import write_cgroup
except ImportError:
    def write_cgroup(mainpid):
        pass


# Debug switch
DISABLE_PERSISTENCE = False
FORCE_RESCRAPE = False
PERIODIC_SCRAPES = False
ENABLE_ALTMAIN = False
ALLOW_COOKIED_COMMUNITY_TAB_SCRAPE = False
SCRAPER_SLEEP_INTERVAL = 120 * 5 / 2
CHANNEL_SCRAPE_LIMIT = 30


downloadmetacmd = "../yt-dlp/yt-dlp.sh -s -q -j --ignore-no-formats-error "
downloadchatprgm = "../downloader.py"
channelscrapecmd = "../scrape_channel_oo.sh"
channelpostscrapecmd = "../scrape_community_tab.sh"
channelmemberscrapecmd = "../scrape_membership_tab.sh"
mainchannelsfile = "./channels.txt"
watchdogprog = "../watchdog.sh"
holoscrapecmd = 'wget -nv --load-cookies=../cookies-schedule-hololive-tv.txt https://schedule.hololive.tv/lives -O auto-lives_tz'


statuses = frozenset(['unknown', 'prelive', 'live', 'postlive', 'upload', 'error'])
progress_statuses = frozenset(['unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'])


# For alt-main usage
is_true_main = True


def _get_member_cookie_file(channel_id: str):
    if not is_true_main and channel_id is not None:
        return f"cookies/{channel_id}.txt"


def get_timestamp_now():
    return dt.datetime.utcnow().timestamp()


class TransitionException(Exception):
    """ Invalid live status transition by setter """
    pass


class Video:
    """ Record the online status of a video, along with the scraper's download stage.
        Metadata from Youtube is also stored when needed.
        video_id is the unique Youtube ID for identifying a video.
    """
    def __init__(self, video_id, *, id_source=None, referrer_channel_id=None):
        self.video_id = video_id
        self.id_source = id_source
        self.referrer_channel_id = referrer_channel_id
        self.status = 'unknown'
        self.progress = 'unscraped'
        self.warned = False
        self.init_timestamp = get_timestamp_now()
        self.transition_timestamp = self.init_timestamp
        self.meta_timestamp = None
        # might delete one
        self.meta = None
        self.rawmeta = None
        # might change
        self.did_discovery_print = False
        self.did_status_print = False
        self.did_progress_print = False
        self.did_meta_flush = False
        self.status_flush_reason = 'new video object'
        self.progress_flush_reason = 'new video object'
        self.meta_flush_reason = 'new video object'
        # declare our possible intent here
        self.next_event_check = 0

    def set_status(self, status: str):
        """ Set the online status (live progress) of a video
            Currently can be any of: 'unknown', 'prelive', 'live', 'postlive', 'upload'.
            Invalid progress transtitions print a warning (except for 'unknown').
        """
        if status not in statuses:
            raise ValueError(f"tried to set invalid status: {status}")

        if status == 'unknown':
            raise TransitionException("status cannot be set to 'unknown', only using reset")

        if status == 'prelive' and self.status in {'live', 'postlive', 'upload'} \
                or status == 'live' and self.status in {'postlive', 'upload'} \
                or status == 'postlive' and self.status in {'upload'}:
            print(f"warning: new video status invalid: transitioned from {self.status} to {status}", file=sys.stderr)
            self.warned = True

        if status == 'postlive' and self.status in {'prelive'}:
            print(f"warning: new video status suspicious: transitioned from {self.status} to {status}", file=sys.stderr)
            self.warned = True

        self.status_flush_reason = getattr(self, 'status_flush_reason', 'new video object (outdated layout!)')

        if status == self.status:
            print(f"warning: new video status suspicious: no change in status: {status}", file=sys.stderr)
            self.warned = True
        else:
            if self.did_status_print:
                self.status_flush_reason = f'status changed: {self.status} -> {status}'
            else:
                self.status_flush_reason += f'; {self.status} -> {status}'
            self.did_status_print = False

        self.transition_timestamp = get_timestamp_now()
        self.status = status

    def set_progress(self, progress: str):
        """ Set the scraper progress of a video
            Currently can be any of: 'unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'
            Invalid progress transtitions throw a TransitionException.
        """
        if progress not in progress_statuses:
            raise ValueError(f"tried to set invalid progress status: {progress}")

        if progress == 'unscraped':
            raise TransitionException("progress cannot be set to 'unscraped', only using reset")

        if progress == 'waiting' and self.progress != 'unscraped' \
                or progress == 'downloading' and self.progress != 'waiting' \
                or progress == 'downloaded' and self.progress != 'downloading' \
                or progress == 'missed' and self.progress not in {'unscraped', 'waiting'} \
                or progress == 'invalid' and self.progress != 'unscraped' \
                or progress == 'aborted' and self.progress == 'downloaded':
            raise TransitionException(f"progress cannot be set to {progress} from {self.progress}")

        self.progress_flush_reason = getattr(self, 'progress_flush_reason', 'new video object (outdated layout!)')

        if progress == self.progress:
            print(f"warning: new progress status suspicious: no change in progress: {progress}", file=sys.stderr)
            self.warned = True
        else:
            if self.did_progress_print:
                self.progress_flush_reason = f'progress changed: {self.progress} -> {progress}'
            else:
                self.progress_flush_reason += f'; {self.progress} -> {progress}'
            self.did_progress_print = False

        self.transition_timestamp = get_timestamp_now()
        self.progress = progress

        if progress in {'unscraped', 'waiting', 'downloading'} and self.status == 'postlive':
            print(f"warning: overriding new progress state due to postlive status: {progress} -> missed", file=sys.stderr)
            self.progress = 'missed'

    def reset_status(self):
        """ Set the status to 'unknown'. Useful for clearing state loaded from disk. """
        self.did_status_print = False
        self.status_flush_reason = f'status reset: {self.status} -> unknown'
        self.status = 'unknown'

    def reset_progress(self):
        """ Set progress to 'unscraped'. Useful for clearing state loaded from disk. """
        self.did_progress_print = False
        self.progress_flush_reason = f'progress reset: {self.progress} -> unscraped'
        self.progress = 'unscraped'

    def prepare_meta(self):
        """ Load meta from disk or fetch it from YouTube. """
        # NOTE: Currently unused.
        if self.meta is None:
            rescrape(self)

            self.rawmeta = self.meta.get('raw')
            if self.rawmeta:
                del self.meta['raw']

            self.meta_timestamp = get_timestamp_now()

    def rescrape_meta(self):
        """ Ignore known meta and fetch meta from YouTube. """
        cookie_file = _get_member_cookie_file(self.referrer_channel_id or self.meta.get('channel_id'))
        scrapers = [rescrape_chatdownloader, rescrape_ytdlp]
        for rescrape in scrapers:
            lastmeta = self.meta
            self.meta = None

            try:
                rescrape(self, cookies=cookie_file)
            except Exception:
                self.meta = lastmeta

            if self.meta:
                rawmeta = self.meta.get('raw')
                if rawmeta:
                    self.rawmeta = rawmeta
                    del self.meta['raw']

                # Avoid a case where failing meta scrapes kept flushing.
                is_simple = self.meta is not None and self.rawmeta is None
                if not is_simple or self.meta != lastmeta:
                    self.meta_timestamp = get_timestamp_now()
                    self.did_meta_flush = False
                    self.meta_flush_reason = 'new meta after rescrape requested'

                break


class Channel:
    """ Tracks basic details about a channel, such as the videos that belong to it. """
    def __init__(self, channel_id):
        self.channel_id = channel_id
        self.videos = set()
        self.init_timestamp = get_timestamp_now()
        self.modify_timestamp = self.init_timestamp
        self.did_discovery_print = False
        self.batching = False
        self.batch = None

    def add_video(self, video: Video):
        """ Add a video to our list, and possibly our current batch
            Modifies timestamp on success
        """
        if video.video_id not in self.videos:
            self.videos.add(video.video_id)
            self.modify_timestamp = get_timestamp_now()
            self.did_discovery_print = False
            if self.batching:
                self.batch.add(video.video_id)

    def add_video_ids(self, video_ids: list):
        """ Add videos to our list, and possibly our current batch
            Modifies timestamp on success
        """
        new_videos = set(video_ids) - self.videos
        if len(new_videos) > 0:
            self.modify_timestamp = get_timestamp_now()
            self.did_discovery_print = False
            if self.batching:
                self.batch |= new_videos

    def start_batch(self):
        """ Declare that the next videos are a new batch """
        if self.batching:
            raise TransitionException("channel batch already started")

        self.batching = True
        self.batch = set()

    def end_batch(self):
        """ Finish declaring that the next videos are a new batch """
        if not self.batching:
            raise TransitionException("channel batch not started")

        self.batching = False

    def clear_batch(self):
        """ Forget a batch (does not affect list of videos) """
        self.batching = False
        self.batch = set()


class AutoScraper:
    """ Main context storing videos and other info """
    LAYOUT_VERSION = 1

    def __init__(self):
        self.lives = {}
        self.channels = {}
        self.events = {}
        self.pids = {}
        self.general_stats = {}  # for debugging
        self.init_timestamp = get_timestamp_now()

    def get_or_init_video(self, /, video_id, *, id_source=None, referrer_channel_id=None):
        video = None
        if video_id not in self.lives:
            video = Video(video_id, id_source=id_source, referrer_channel_id=referrer_channel_id)
            self.lives[video_id] = video
        else:
            video = self.lives[video_id]

        return video

    def update_lives_status(self, /):
        if not is_true_main:
            self.update_lives_status_cookied()
            return

        with open("discovery.txt", "a") as dlog:
            try:
                self.update_lives_status_holoschedule(dlog=dlog)
            except Exception:
                print("warning: exception during holoschedule scrape. Network error?")
                traceback.print_exc()

            try:
                self.update_lives_status_urllist(dlog=dlog)
            except Exception:
                print("warning: exception during urllist scrape. Network error?")
                traceback.print_exc()

            try:
                self.update_lives_status_channellist(dlog=dlog)
            except Exception:
                print("warning: exception during channellist scrape. Network error?")
                traceback.print_exc()

    def update_lives_status_cookied(self, /):
        """ Update membership pages using cookies """
        if is_true_main:
            return

        self.update_lives_status_channellist(is_membership=True)

    def update_lives_status_holoschedule(self, /, *, dlog: IO = None) -> None:
        """ Process the holoschedule, which updates on a short delay. """
        # Find all valid hyperlinks to youtube videos
        soup = get_hololivetv_html()
        newlives = 0
        knownlives = 0

        if dlog is None:
            dlog = sys.stdout

        for link in soup.find_all('a'):
            # Extract any link
            href = link.get('href')
            video_id = extract_video_id_from_yturl(href, strict=True)

            if video_id is None:
                continue

            if video_id not in self.lives:
                recall_video(video_id, context=self, filter_progress=True)

            video = self.get_or_init_video(video_id, id_source='holoschedule')
            if video.progress == 'unscraped':
                print("discovery: new live listed:", video_id, file=dlog, flush=True)
                newlives += 1
            else:
                # known (not new) live listed
                knownlives += 1

        print("discovery: holoschedule: new lives:", str(newlives))
        print("discovery: holoschedule: known lives:", str(knownlives))

    def update_lives_status_urllist(self, *, dlog: IO = None):
        """ Process a url file (TODO).
            Can be called standalone.
        """
        # TODO
        pass

    def update_lives_status_channellist(self, *, dlog: IO = None, is_membership=False) -> None:
        """ Read channels.txt for a list of channel IDs to process. """
        if dlog is None:
            dlog = sys.stdout

        channels_file = mainchannelsfile

        if is_membership:
            channels_file = 'channels-cookied.txt'

        try:
            if os.path.exists(channels_file):
                with open(channels_file) as channellist:
                    for channel_id in [x.strip().split()[0] for x in channellist.readlines()]:
                        self.scrape_and_process_channel(channel_id=channel_id, dlog=dlog)

        except Exception:
            print("warning: unexpected error with processing channels.txt", file=sys.stderr)
            traceback.print_exc()

    # TODO: rewrite
    def process_channel_videos_ytdlp(self, /, channel: Channel, *, dlog: IO = None, is_membership=False):
        """ Read scraped channel video list, proccess each video ID, and persist the meta state. """
        if dlog is None:
            dlog = sys.stdout

        newlives = 0
        knownlives = 0
        numignores: Dict = {}
        channel_id = channel.channel_id
        channel.did_discovery_print = True

        allurl_file = "channel-cached/" + channel_id + ".url.all"
        if is_membership:
            allurl_file = "channel-cached/" + channel_id + ".url.mem.all"

        channel.start_batch()

        try:
            with open(allurl_file) as urls:
                for video_id in [f.split(" ")[1].strip() for f in urls.readlines()]:
                    # Process each recent video
                    if video_id not in self.lives:
                        recall_video(video_id, context=self, filter_progress=True)

                    video = self.lives[video_id]
                    channel.add_video(video)

                    if not channel.did_discovery_print:
                        print("discovery: new live listed: " + video_id + " on channel " + channel_id, file=dlog, flush=True)
                        # TODO: accumulate multiple videos at once.
                        channel.did_discovery_print = True
                        newlives += 1
                    else:
                        # known (not new) live listed (channel unaware)
                        knownlives += 1

                    saved_progress = video.progress

                    if not FORCE_RESCRAPE and saved_progress in {'downloaded', 'missed', 'invalid', 'aborted'}:
                        numignores[saved_progress] = numignores.setdefault(saved_progress, 0) + 1

                        delete_ytmeta_raw(video, context=self, suffix=" (channel)")

                        continue

                    cache_miss = False

                    # process precached meta
                    if video.meta is None and not is_membership:
                        # We may be reloading old URLs after a program restart
                        print("ytmeta cache miss for video " + video_id + " on channel " + channel_id)
                        cache_miss = True
                        rescrape(video)
                        if video.meta is None:
                            # scrape failed
                            continue
                        video.rawmeta = video.meta.get('raw')
                        video.did_meta_flush = False
                        video.meta_flush_reason = 'new meta (yt-dlp source, channel task origin, after cache miss)'
                    elif video.meta is None:
                        print("ytmeta missing for member video " + video_id + " on channel " + channel_id)
                        rescrape(video, cookies=_get_member_cookie_file(channel_id))
                        if video.meta is None:
                            if not video.did_meta_flush:
                                print("warning: didn't flush meta for channel member video; flushing now", file=sys.stderr)
                                persist_ytmeta(video, fresh=True)
                            # scrape failed
                            continue
                        video.rawmeta = video.meta.get('raw')
                        video.did_meta_flush = False
                        video.meta_flush_reason = 'new meta (yt-dlp source, channel task origin, after cache miss (cookied))'

                    # There's an optimization opportunity for reducing disk flushes here, but we forego it

                    # has parity with maybe_rescrape(); should only need to be called on new videos
                    if video.progress == 'unscraped':
                        process_ytmeta(video)

                    # has parity with maybe_rescrape() as well
                    if cache_miss or (saved_progress not in {'missed', 'invalid'} and saved_progress != video.progress):
                        persist_meta(video, context=self, fresh=True)

                    if not video.did_meta_flush:
                        print("warning: didn't flush meta for channel video; flushing now", file=sys.stderr)
                        persist_ytmeta(video, fresh=True)

        except IOError:
            print("warning: unexpected I/O error when processing channel scrape results", file=sys.stderr)
            traceback.print_exc()

        channel.end_batch()

        if len(channel.batch) > 0:
            print("discovery: channels list: new lives on channel " + channel_id + " : " + str(newlives))
            print("discovery: channels list: known lives on channel " + channel_id + " : " + str(knownlives))
            for progress, count in numignores.items():
                print("discovery: channels list: skipped ytmeta fetches on channel " + channel_id + " : " + str(count) + " skipped due to progress state '" + progress + "'")

        channel.clear_batch()

    def scrape_and_process_channel(self, channel_id, *, dlog: IO = None) -> None:
        """ Scrape a channel, with fallbacks.
            Can be called standalone.
        """
        channel = None
        use_ytdlp = False

        if dlog is None:
            dlog = sys.stdout

        if channel_id in self.channels:
            channel = self.channels[channel_id]
        else:
            channel = Channel(channel_id)
            self.channels[channel_id] = channel
            # use chat_downloader to get initial video list
            print("New channel: " + channel.channel_id)

        # alt-main only
        if not is_true_main:
            use_ytdlp = True

        if not use_ytdlp:
            try:
                self.scrape_and_process_channel_chatdownloader(channel, dlog=dlog)
            except Exception:
                print("failed to scrape channel list with chat_downloader:", channel_id, file=sys.stderr)
                traceback.print_exc()
                use_ytdlp = True

        if use_ytdlp:
            is_membership = not is_true_main
            self.invoke_channel_scraper_ytdlp(channel, membership_scrape=is_membership)
            self.process_channel_videos_ytdlp(channel, dlog=dlog, is_membership=is_membership)

        # Scrape community tab page for links (esp. member stream links)
        # Currently only try this when cookies are provided.
        # TODO: use membership page instead, since it only includes (recent?) member videos and posts.
        # I believe this is a new feature by YouTube (~Nov 2021).
        if ALLOW_COOKIED_COMMUNITY_TAB_SCRAPE:
            if os.path.exists(channel_id + ".txt"):
                self.invoke_channel_scraper_ytdlp(channel, community_scrape=True)
                self.process_channel_videos_ytdlp(channel, dlog=dlog)

    def scrape_and_process_channel_chatdownloader(self, /, channel: Channel, *, dlog: IO = None):
        """ Use chat_downloader's get_user_videos() to quickly get channel videos and live statuses. """
        if dlog is None:
            dlog = sys.stdout

        downloader = ChatDownloader()

        # Forcefully create a YouTube session
        youtube: YouTubeChatDownloader = downloader.create_session(YouTubeChatDownloader)

        limit = CHANNEL_SCRAPE_LIMIT
        count = 0
        perpage_count = 0
        valid_count = 0
        skipped = 0

        seen_vids: Set[str] = set()
        lives = self.lives

        # We don't just check 'all' since the list used may be slow to update.
        for video_status in ['upcoming', 'live', 'all']:
            perpage_count = 0
            time.sleep(0.1)
            for basic_video_details in youtube.get_user_videos(channel_id=channel.channel_id, video_status=video_status, params={'max_attempts': 3}):
                status = 'unknown'
                status_hint: Optional[str] = None
                just_scraped = False

                video_id = basic_video_details.get('video_id')

                try:
                    status_hint = basic_video_details['view_count'].split()[1]
                    if status_hint == "waiting":
                        status = 'prelive'
                    elif status_hint == "watching":
                        status = 'live'
                    elif status_hint == "views":
                        pass
                    else:
                        print(f"warning: could not understand status hint ({status_hint = })", file=sys.stderr)
                        raise RuntimeError('could not extract status hint')

                except KeyError:
                    if video_id is not None and video_id in lives and lives[video_id].progress not in {'unscraped', 'aborted'} and lives[video_id].status not in {'postlive', 'upload'}:
                        print(f"warning: status hint extraction: unexpected KeyError... {count = } {perpage_count = } (+1) ... {valid_count = } {skipped = } {limit = } ... {seen_vids = } ... {basic_video_details = })", file=sys.stderr)
                        traceback.print_exc()
                    elif video_id not in lives:
                        video = self.get_or_init_video(video_id, id_source=f'channel:{video_status}', referrer_channel_id=channel.channel_id)
                        print(f"warning: status hint extraction: no status hint and new video, doing direct video scrape: {video_id}", file=sys.stderr)
                        rescrape_chatdownloader(video, channel=channel, youtube=youtube)
                        just_scraped = True
                        if video.meta is not None:
                            live_status = video.meta.get('live_status')
                            if live_status == 'is_upcoming':
                                status = 'prelive'
                            elif live_status == 'is_live':
                                status = 'live'
                    else:
                        # 'waiting' may be hidden on the player response page (possibly a server bug, but could also be intentional)
                        print(f"warning: status hint extraction: unexpected KeyError, already scraped, not live... {basic_video_details = })", file=sys.stderr)

                except Exception:
                    print("warning: could not extract status hint", file=sys.stderr)
                    raise

                perpage_count += 1
                if perpage_count >= limit:
                    if video_id in seen_vids or status == 'unknown' or (video_id in lives and lives[video_id].progress != 'unscraped'):
                        # would continue
                        print(f"perpage limit of {limit} reached:", video_status)
                        if video_id not in seen_vids:
                            count += 1
                        if status != 'unknown' and not (video_id in lives and lives[video_id].progress != 'unscraped'):
                            skipped += 1
                        break

                if video_id in seen_vids:
                    continue
                else:
                    count += 1

                if status == 'unknown':
                    # ignore past streams/uploads
                    continue

                valid_count += 1

                if video_id in lives and lives[video_id].progress != 'unscraped':
                    skipped += 1
                    continue

                if status != 'unknown':
                    print(f"discovery: new live listed (chat_downloader channel extraction, status: {status}): " + video_id, file=sys.stdout, flush=True)
                    print(f"discovery: new live listed (chat_downloader channel extraction, status: {status}): " + video_id, file=dlog, flush=True)

                video = self.get_or_init_video(video_id, id_source=f'channel:{video_status}', referrer_channel_id=channel.channel_id)

                channel.add_video(video)

                # rescrape and process a new video
                if not just_scraped:
                    rescrape_chatdownloader(video, channel=channel, youtube=youtube)

                persist_meta(video, context=self, fresh=True, clobber_pid=False)

                if perpage_count >= limit:
                    print(f"perpage limit of {limit} reached:", video_status)
                    break

            if count >= limit * 3:
                print(f"limit of {limit} reached")
                break

        print(f"discovery: channels list (via chat_downloader): channel {channel.channel_id} new upcoming/live lives: " + str(valid_count) + "/" + str(count) + " (" + str(skipped) + " known)")

    def invoke_channel_scraper_ytdlp(self, /, channel: Channel, *, community_scrape=False, membership_scrape=False):
        """ Scrape the channel for latest videos and batch-fetch meta state. """
        # Note: some arbitrary limits are set in the helper program that may need tweaking.
        allmeta_file = "channel-cached/" + channel.channel_id + ".meta.new"

        if not community_scrape and not membership_scrape:
            print("Scraping channel " + channel.channel_id)
            subprocess.run(channelscrapecmd + " " + channel.channel_id, shell=True)
        elif not membership_scrape:
            print("Scraping channel community pages " + channel.channel_id)
            subprocess.run(channelpostscrapecmd + " " + channel.channel_id, shell=True)
        else:
            print("Scraping channel membership pages " + channel.channel_id)
            subprocess.run(channelmemberscrapecmd + " " + channel.channel_id, shell=True)
            allmeta_file = "channel-cached/" + channel.channel_id + ".meta.mem.new"

        with open(allmeta_file) as allmeta:
            metalist = []

            for jsonres in allmeta.readlines():
                try:
                    metalist.append(populate_meta_fields_ytdlp(json.loads(jsonres)))
                except Exception:
                    if community_scrape:
                        print("warning: exception in channel post scrape task (corrupt meta?)", file=sys.stderr)
                    else:
                        print("warning: exception in channel scrape task (corrupt meta?)", file=sys.stderr)
                    traceback.print_exc()

            for ytmeta in metalist:
                video_id = ytmeta["id"]
                recall_video(video_id, context=self, filter_progress=True)
                video = self.lives.get(video_id)
                if video and video.meta is None:
                    video.meta = ytmeta
                    video.rawmeta = ytmeta.get('raw')
                    video.did_meta_flush = False
                    video.meta_flush_reason = 'new meta (yt-dlp source, channel task origin)'
                else:
                    if community_scrape:
                        print("ignoring ytmeta from channel post scrape")
                    else:
                        print("ignoring ytmeta from channel scrape")


main_autoscraper = AutoScraper()


# video statuses:
# unknown: not yet scraped
# prelive: scheduled live
# live: in-progress live
# postlive: completed/missed live
# upload: not a livestream


# progress statuses:

# add -> unscraped
# unscraped -> waiting if scheduled
# unscraped -> downloading if downloader invoked (I don't think this is used)
# unscraped -> missed if was live
# unscraped -> invalid if not a live (was uploaded)
# waiting -> downloading when the chat is available, downloader invoked
# waiting -> missed if downloader was unable to invoke and finished airing
# downloading -> downloaded if downloader completes.

# unscraped: needs scrape
# waiting: future-scheduled live, not yet downloading or downloaded
# downloading: chat downloader invoked successfully
# downloaded: chat downloader completed after successful invocation
# missed: already aired live, we skip
# invalid: isn't a livestream
# aborted: could not process video (scrape failed?)


# YTMeta:
# raw: json output of the yt-dl program
# id:
# title:
# description:
# duration:
# uploader: (name)
# channel_id:
# is_livestream:
# is_live:
# live_starttime:
# live_endtime:
# is_upcoming:


def get_hololivetv_html():
    """ Get the latest html page of the older site's schedule """
    subprocess.run(holoscrapecmd, shell=True)

    html_doc = ''
    with open("auto-lives_tz", "rb") as fp:
        html_doc = fp.read()

    soup = BeautifulSoup(html_doc, 'html.parser')
    with open("auto-lives_tz", "wb") as fp:
        fp.write(soup.prettify().encode())

    return soup


def rescrape_chatdownloader(video: Video, *, channel=None, youtube=None, cookies=None) -> None:
    """ rescrape_ytdlp, but using chat_downloader
        Interpret yt-dlp rawmeta.
        Populates meta fields.
    """
    video_id = video.video_id
    video_data, player_response, status = invoke_scraper_chatdownloader(video_id, youtube=youtube, skip_status=False, cookies=cookies)

    # keep only known useful fields, junk spam/useless fields
    old_player_response = player_response
    player_response = {}
    for key in ['playabilityStatus', 'videoDetails', 'microformat']:
        player_response[key] = old_player_response[key]
    for key in ['streamingData']:
        player_response[key] = old_player_response.get(key)
    del old_player_response

    meta = populate_meta_fields_chatdownloader(player_response=player_response, video_data=video_data, channel=channel, video_id=video_id)

    video.set_status(status)

    video.did_meta_flush = False
    word = None
    if video.meta is None:
        word = 'new'
    else:
        word = 'updated'
    if channel is None:
        video.meta_flush_reason = f'{word} meta (chat_downloader source, unspecified task origin)'
    else:
        video.meta_flush_reason = f'{word} meta (chat_downloader source, channel task origin)'
    del word

    video.meta = meta
    rawmeta = meta.get('raw')
    if not rawmeta:
        # Note: rawmeta may be older than meta, but it's better than being lost.
        if video.progress not in {'downloading', 'downloaded', 'missed'}:
            video.set_progress('aborted')
    else:
        video.rawmeta = rawmeta
    video.meta_timestamp = get_timestamp_now()

    try:
        del meta['raw']
    except KeyError:
        pass


def populate_meta_fields_chatdownloader(*, player_response: Dict[str, Any], video_data: Dict[str, Any], channel: Channel = None, video_id: str) -> Dict[str, Any]:
    """ Interpret chat_downloader rawmeta.
        Populates meta fields.
    """
    microformat = player_response['microformat']['playerMicroformatRenderer']
    video_details = player_response['videoDetails']

    # "export" the fields manually here
    meta: Dict[str, Any] = {}

    meta['_scrape_provider'] = 'chat_downloader'
    meta['id'] = video_id
    meta['referrer_channel_id'] = channel and channel.channel_id
    meta['channel_id'] = video_details['channelId']
    meta['title'] = microformat['title']['simpleText']
    meta['raw'] = player_response  # I think this is different from yt-dlp infodict output
    meta['description'] = microformat['description']['simpleText']
    meta['uploader'] = video_data['author']
    meta['duration'] = video_data['duration']

    meta['is_live'] = video_details.get('isLive') is True
    meta['is_upcoming'] = video_details.get('isUpcoming') is True
    meta['is_livestream'] = video_details.get('isLiveContent') is True

    try:
        meta['live_starttime'] = int(dt.datetime.fromisoformat(microformat['liveBroadcastDetails']['startTimestamp']).timestamp() + 0.1)
    except Exception:
        meta['live_starttime'] = None

    try:
        meta['live_endtime'] = int(dt.datetime.fromisoformat(microformat['liveBroadcastDetails']['endTimestamp']).timestamp() + 0.1)
    except Exception:
        meta['live_endtime'] = None

    if meta['is_live']:
        meta['live_status'] = 'is_live'
    elif meta['is_upcoming']:
        meta['live_status'] = 'is_upcoming'
    elif meta['is_livestream']:
        meta['live_status'] = 'was_live'
    else:
        meta['live_status'] = 'not_live'

    return meta


def invoke_scraper_chatdownloader(video_id: str, *, youtube=None, skip_status=False, cookies=None):
    """ Like invoke_scraper_ytdlp, but use chat_downloader's python interface instead of forking and calling yt-dlp.
        Try to export the status for the autoscraper as well.
        Returns raw YouTube data and the deduced status.
    """
    if youtube is None:
        downloader = ChatDownloader(cookies=cookies)
        youtube = downloader.create_session(YouTubeChatDownloader)

    print(f'scraper chatdownloader: {video_id = } {cookies = }')
    video_data, player_response, *_ = youtube._parse_video_data(video_id, params={'max_attempts': 2})

    scraper_status: Optional[str] = None
    if not skip_status:
        details = youtube.get_video_data(video_id)
        status = details.get('status')
        video_type = details.get('video_type')
        if video_type not in {'premiere', 'video'} or (video_type == 'premiere' and details.get('continuation_info') == {}):
            scraper_status = 'upload'
        elif status == 'upcoming':
            scraper_status = 'prelive'
        elif status == 'live':
            scraper_status = 'live'
        elif status == 'past':
            scraper_status = 'postlive'
        else:
            scraper_status = 'error'

    return video_data, player_response, scraper_status


def persist_basic_state(video: Video, *, context: AutoScraper, clobber=True, clobber_pid=None):
    """ Write status and progress info to state file, flush pid; ytmeta is excluded """
    video_id = video.video_id
    statefile = 'by-video-id/' + video_id
    pidfile = 'pid/' + video_id

    if not _check_meta_persistence_enabled(video):
        return

    state = {}
    state['status'] = video.status
    state['progress'] = video.progress

    if clobber_pid is None:
        clobber_pid = clobber

    if clobber or not os.path.exists(statefile):
        print('Updating statefile ' + statefile)
        with open(statefile, 'wb') as fp:
            fp.write(json.dumps(state, indent=1).encode())

    if clobber_pid or not os.path.exists(pidfile):
        with open(pidfile, 'wb') as fp:
            if context.pids.get(video_id) is not None:
                # Write dlpid to file
                fp.write(str(context.pids[video_id][1]).encode())


def _check_meta_persistence_enabled(video: Video):
    """ Check if DISABLE_PERSISTENCE is set and run handling.
        Return true if it is not set.
    """
    statefile = 'by-video-id/' + video.video_id

    # Debug switch
    if DISABLE_PERSISTENCE:
        print('NOT updating ' + statefile)
        if not video.did_meta_flush:
            print("  meta flush reason (no-op):", video.meta_flush_reason)
        else:
            print("  meta flush reason (no-op, already attempted?):", video.meta_flush_reason)

        video.did_meta_flush = True
        video.meta_flush_reason = 'no reason set (no-op enabled)'

    return not DISABLE_PERSISTENCE


def persist_meta(video: Video, *, context: AutoScraper, fresh=False, clobber=True, clobber_pid=None):
    """ Persist state and ytmeta at once. """
    if not _check_meta_persistence_enabled(video):
        return

    persist_basic_state(video, context=context, clobber=clobber, clobber_pid=clobber_pid)
    persist_ytmeta(video, fresh=fresh, clobber=clobber)


def persist_ytmeta(video: Video, *, fresh=False, clobber=True):
    """ Persist ytmeta only. """
    metafile = 'by-video-id/' + video.video_id

    # Write ytmeta to a separate file (to avoid slurping large amounts of data)
    if video.meta is not None:
        ytmeta = {}
        ytmeta['ytmeta'] = video.meta
        ytmeta['ytmeta']['raw'] = video.rawmeta
        if video.rawmeta is None:
            ytmeta['ytmeta']['raw'] = video.meta.get('raw')

        metafileyt = metafile + ".meta"
        metafileyt_status = metafileyt + "." + video.status
        if video.rawmeta is None:
            metafileyt_status += ".simple"

        try:
            if clobber or not os.path.exists(metafileyt):
                print('Updating ' + metafileyt)
                with open(metafileyt, 'wb') as fp:
                    fp.write(json.dumps(ytmeta, indent=1).encode())

            if clobber or not os.path.exists(metafileyt_status):
                try:
                    bugtest1 = metafileyt + "." + 'prelive'
                    bugtest2 = metafileyt + "." + 'live'
                    bugtest3 = metafileyt + "." + 'postlive'
                    if os.path.exists(bugtest3) and metafileyt_status != bugtest3:
                        print('warning: redundant meta status write:', metafileyt_status, file=sys.stderr)
                    # I'll figure out how to do this with warnings eventually... maybe.
                    # Hunt down a likely bug.
                    if os.path.exists(bugtest3) and metafileyt_status == bugtest2:
                        raise RuntimeError(f'illegal meta write (bug): {metafileyt_status} written after {bugtest3})')
                    if os.path.exists(bugtest3) and metafileyt_status == bugtest1:
                        raise RuntimeError(f'illegal meta write (bug): {metafileyt_status} written after {bugtest3})')
                    if os.path.exists(bugtest2) and metafileyt_status == bugtest1:
                        raise RuntimeError(f'illegal meta write (bug): {metafileyt_status} written after {bugtest2})')

                    print('Updating ' + metafileyt_status)
                    with open(metafileyt_status, 'wb') as fp:
                        fp.write(json.dumps(ytmeta, indent=1).encode())
                except RuntimeError:
                    traceback.print_exc()
            else:
                print('NOT updating (exists and noclobber set): ' + metafileyt_status)

        finally:
            try:
                # Since we don't deep-copy, don't keep 'raw' in the meta dict.
                if video.rawmeta is not None:
                    del video.meta['raw']
            except KeyError:
                pass

    if not video.did_meta_flush:
        print("  meta flush reason:", video.meta_flush_reason)
    else:
        print("  meta flush reason (already flushed?):", video.meta_flush_reason)
        if fresh:
            # "fresh" is a holdover from process_dlpid_queue, which passed fresh=False back when progress had a copy.
            # Nearly all calls to this function use it, so repurpose it as a debugging hint.
            print("    meta was said to be fresh, did we lie?")

    video.did_meta_flush = True
    video.meta_flush_reason = 'no reason set'


# TODO: replace recall_meta with recall_video
def recall_video(video_id: str, *, context: AutoScraper, filter_progress=False, id_source=None, referrer_channel_id=None):
    """ Read status, progress for video_id.
        If filter_progress is set to True, avoid ytmeta loads for certain progress states,
        unless unconditional rescraping is set.
        meta['raw'] is moved to rawmeta if needed.
        context: used for pid recall
    """
    # Not cached in memory, look for saved state.
    metafile = 'by-video-id/' + video_id
    metafileyt = metafile + ".meta"
    valid_meta = os.path.exists(metafile)
    valid_ytmeta = os.path.exists(metafileyt)
    meta = typing.cast(Dict[str, str], None)
    ytmeta = typing.cast(Dict[str, Dict[str, Any]], None)
    should_ignore = False

    if valid_meta:
        # Query saved state if it is not loaded
        with open(metafile, 'rb') as fp:
            try:
                meta = json.loads(fp.read())
                valid_meta = meta['status'] in statuses and meta['progress'] in progress_statuses

            except (json.decoder.JSONDecodeError, KeyError):
                valid_meta = False

        # Reduce memory usage by not loading ytmeta for undownloadable videos
        if valid_meta and filter_progress:
            should_ignore = meta['status'] in {'postlive', 'upload'} and meta['progress'] != 'unknown'
            should_ignore = should_ignore or meta['progress'] in {'downloaded', 'missed', 'invalid', 'aborted'}

        # note: FORCE_RESCRAPE might clobber old ytmeta if not loaded (bad if the video drastically changes or goes unavailable)
        if valid_ytmeta and not should_ignore:
            with open(metafileyt, 'rb') as fp:
                try:
                    ytmeta = json.loads(fp.read())
                    valid_ytmeta = 'ytmeta' in ytmeta

                except (json.decoder.JSONDecodeError, KeyError):
                    valid_ytmeta = False

    # This has to be conditional, unless we want old references to be silently not updated and have tons of debugging follow.
    video = context.get_or_init_video(video_id, id_source=id_source, referrer_channel_id=referrer_channel_id)

    if valid_meta:
        # Commit status to runtime tracking (else we would discard it here)
        # Direct assignment here to avoid checks, might rewrite later
        video.status = meta['status']
        video.progress = meta['progress']

        if valid_ytmeta and not should_ignore:
            video.meta = typing.cast(Dict[str, Any], ytmeta['ytmeta'])
            video.rawmeta = video.meta.get('raw')
            if video.rawmeta is not None:
                del video.meta['raw']

        # unmigrated (monolithic file) format
        elif 'ytmeta' in meta:
            video.meta = typing.cast(Dict[str, Any], meta['ytmeta'])
            video.rawmeta = video.meta.get('raw')
            if video.rawmeta is not None:
                del video.meta['raw']

            if DISABLE_PERSISTENCE:
                return

            print('notice: migrating ytmeta in status file to new file right now: ' + metafile)
            persist_meta(video, context=context, fresh=True)

            if should_ignore:
                delete_ytmeta_raw(video, suffix=" (meta recall)")


def process_ytmeta(video: Video):
    """ Set status, initial progress from meta """
    if video.meta is None:
        raise RuntimeError('precondition failed: called process_ytmeta but ytmeta for video ' + video.video_id + ' not found.')

    if video.meta['is_upcoming']:
        # note: premieres can also be upcoming but are not livestreams.
        video.set_status('prelive')
        if video.progress == 'unscraped':
            video.set_progress('waiting')

    elif video.meta['is_live']:
        video.set_status('live')
        if video.progress == 'unscraped':
            video.set_progress('waiting')

    elif video.meta['is_livestream'] or video.meta['live_endtime']:
        # note: premieres also have a starttime and endtime
        video.set_status('postlive')
        if video.progress == 'unscraped':
            video.set_progress('missed')

    else:
        video.set_status('upload')
        video.set_progress('invalid')


def check_periodic_event(video: Video, *, context: AutoScraper):
    """ Install or run periodic-rescrape handler for a video """
    try:
        if video.status == 'prelive':
            if (id(context.lives[video.video_id]) != id(video)):
                # The foreach loop in main creates the name video_id that is
                # not deleted after the loop, which for some incredibly unknown
                # reason (implicit nonlocal scope/namespace lookup???) defines
                # the name for functions that are called after the loop.
                print("what the fuck.")
                print("lives.....")
                print(context.lives)
                sys.exit(1)
            if video.video_id in context.events:
                try:
                    # Don't immediately rescrape if we literally just scraped meta.
                    # This may still set the check time to a past timestamp if we hadn't just scraped.
                    next_check = max((video.meta_timestamp + 60 * 5), video.next_event_check)
                    video.next_event_check = next_check
                except Exception:
                    print('warning: periodic rescrape rescheduling failed')
            else:
                schedule_periodic_rescrape(video.video_id, context=context)

            if video.video_id not in context.events or len(context.events[video.video_id]) == 0:
                print('warning: scheduling apparently failed:', video.video_id, file=sys.stderr)

            # If the event check time is in the past, the handler will run.
            run_periodic_rescrape_handler(video.video_id, context=context)

    except Exception:
        print('warning: running periodic rescrape event failed:', video.video_id, file=sys.stderr)
        traceback.print_exc()


def maybe_rescrape(video: Video, *, context: AutoScraper):
    saved_progress = video.progress
    if video.progress == 'unscraped':
        video.rescrape_meta()
        if video.meta is None:
            # all scrapes failed?
            return

        process_ytmeta(video)

        # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
        if saved_progress not in {'missed', 'invalid'} and saved_progress != video.progress:
            persist_meta(video, context=context, fresh=True)

    if PERIODIC_SCRAPES:
        check_periodic_event(video, context=context)


def maybe_rescrape_initially(video: Video, *, context: AutoScraper):
    if video.progress in {'waiting', 'downloading'}:
        # Recover from crash or interruption
        print(f"(initial check) video {video.video_id}: resetting progress after possible crash: {video.progress} -> unscraped")
        video.reset_progress()

    if video.progress in {'missed', 'aborted'} and video.status in {'unknown', 'prelive'}:
        # Recover from potential corruption or bug
        print(f"(initial check) video {video.video_id}: resetting progress after possible bug: {video.progress} -> unscraped. found status: {video.status}")
        video.reset_progress()

    if video.progress == 'unscraped' or FORCE_RESCRAPE:
        video.rescrape_meta()
        if video.meta is None:
            # initial scrape failed
            return

        process_ytmeta(video)

    # Redundant, but purges corruption
    persist_meta(video, context=context, fresh=True)

    if PERIODIC_SCRAPES:
        check_periodic_event(video, context=context)


def populate_meta_fields_ytdlp(jsonres) -> Dict[str, Any]:
    """ Interpret yt-dlp rawmeta.
        Populates meta fields.
    """
    ytmeta: Dict[str, Any] = {}
    ytmeta['_scrape_provider'] = 'yt-dlp'
    ytmeta['raw'] = jsonres
    ytmeta['id'] = jsonres['id']
    ytmeta['title'] = jsonres['title']
    ytmeta['description'] = jsonres['description']
    ytmeta['uploader'] = jsonres['uploader']
    ytmeta['channel_id'] = jsonres['channel_id']
    ytmeta['duration'] = jsonres['duration']

    try:
        # Fields from my yt-dlp fork's experimental patches
        ytmeta['is_live'] = jsonres['is_live']
        ytmeta['live_starttime'] = jsonres['live_starttime']
        ytmeta['live_endtime'] = jsonres['live_endtime']
        ytmeta['is_upcoming'] = jsonres['is_upcoming']
        ytmeta['is_livestream'] = jsonres['was_live']

    except KeyError:
        # yt-dlp introduced their own new metadata fields for livestreams, try those.
        # Note that some data, like the endtime, can't be directly obtained. Also,
        # ISO-8601 times for starttime/endtime have been converted to epoch timestamps.
        try:
            # Old field but repurposed to strictly match its name.
            ytmeta['is_livestream'] = jsonres['was_live']

            # Refetch using possibly missing new fields
            ytmeta['is_livestream'] = 'not_live' not in jsonres['live_status']

            if 'track' in jsonres:
                # Should be a song, so likely (certainly?) a premiere
                ytmeta['is_livestream'] = False

            # Reliable, except in the case of "late" livestreams (where it seems to be missing).
            ytmeta['live_starttime'] = jsonres['release_timestamp']

            # The duration provided by Youtube might not be the broadcast duration;
            # further testing is required. We don't rely on the duration though
            # except for saving finished stream metadata, which isn't done automatically.
            if ytmeta['live_starttime'] is not None and bool(ytmeta['duration']):
                ytmeta['live_endtime'] = ytmeta['live_starttime'] + ytmeta['duration']

            else:
                ytmeta['live_endtime'] = None

            # Fields is_upcoming and is_live have been merged into a string field.
            ytmeta['live_status'] = jsonres['live_status']

            if ytmeta['live_status'] == 'is_live':
                ytmeta['is_live'] = True
            elif ytmeta['live_status'] in {'is_upcoming', 'was_live', 'not_live'}:
                ytmeta['is_live'] = False
            else:
                # live_status is None or set to an unknown value
                ytmeta['is_live'] = ytmeta['live_status'] != 'is_upcoming' and jsonres['live_endtime'] is None

            if 'is_upcoming' not in ytmeta:
                ytmeta['is_upcoming'] = ytmeta['live_status'] == 'is_upcoming'

        except (TypeError, KeyError):
            print("warning: exporting ytmeta fields not fully successful, expect this download to fail:", ytmeta.get('id'), file=sys.stderr)
            ytmeta['is_livestream'] = ytmeta.get('is_livestream')
            ytmeta['live_starttime'] = ytmeta.get('live_starttime')
            ytmeta['live_endtime'] = ytmeta.get('live_endtime')
            ytmeta['live_status'] = ytmeta.get('live_status')
            ytmeta['is_live'] = ytmeta.get('is_live')
            # last-ditch effort to avoid missing a stream
            ytmeta['is_upcoming'] = ytmeta.get('is_upcoming') or not bool(ytmeta['duration'])

    return ytmeta


def rescrape_ytdlp(video: Video, cookies: str = None) -> None:
    """ Invoke the scraper, yt-dlp, on a video now.
        Sets a restructured json result as meta.
    """
    jsonres = invoke_scraper_ytdlp(video.video_id, cookies=cookies)
    if jsonres is None:
        # Mark as aborted here, before processing
        if video.progress not in {'downloading', 'downloaded', 'missed'}:
            video.set_progress('aborted')

        return None

    meta = populate_meta_fields_ytdlp(jsonres)
    video.did_meta_flush = False
    if video.meta is None:
        video.meta_flush_reason = 'new meta (yt-dlp source, unspecified task origin)'
    else:
        video.meta_flush_reason = 'updated meta (yt-dlp source, unspecified task origin)'

    video.meta = meta


def invoke_scraper_ytdlp(video_id: str, cookies: str = None) -> Optional[Dict[str, Any]]:
    """ Call yt-dlp to get new rawmeta. Cookies are stictly for alt-main (member) cookies """
    proc = None
    try:
        cmdline = downloadmetacmd + "-- " + video_id
        if cookies is not None:
            if is_true_main:
                print('warning: rejected cookied scrape attempt')
                cookies = None
            else:
                if os.path.exists(cookies):
                    cmdline = downloadmetacmd + f"--cookies {cookies} -- " + video_id
                else:
                    print(f'warning: cookied scrape attempt with missing cookie file: {cookies}')
                    cookies = None

        print(cmdline.split())
        proc = subprocess.run(cmdline.split(), capture_output=True)
        with open('outtmp', 'wb') as fp:
            fp.write(proc.stdout)
        if len(proc.stdout) == 0:
            print(b"scraper error: no stdout! stderr=" + proc.stderr)

            return None

        return json.loads(proc.stdout)

    except Exception:
        print("warning: exception thrown during scrape task. printing traceback...", file=sys.stderr)
        traceback.print_exc()
        if proc:
            print("stdout dump for failed scrape, for video " + video_id + ":", file=sys.stderr)
            print(proc.stdout, file=sys.stderr)
            print("end of stdout dump for failed scrape:", file=sys.stderr)

        return None


def safen_path(s):
    try:
        # The slice is to avoid long fields hitting path limits, albeit ineffectively.
        return str(s).replace(':', '_').replace('/', '_').replace(' ', '_')[0:100]

    except Exception:
        print("warning: string safening failed, returning dummy value...")

        return ""


def schedule_periodic_rescrape(video_id, *, context: AutoScraper):
    handler_id = 'periodic_rescrape'
    try:
        if video_id not in context.lives:
            print('warning: failed to schedule rescrape: video_id not found:', video_id)
            return

        def time_func() -> None:
            print('running time func', video_id)
            video = context.lives[video_id]
            meta = meta_load_fast(video_id)
            if meta is None:
                print('warning: keep func: fast meta load failed:', video_id)
            nowtime = get_timestamp_now()

            starttime = None
            try:
                starttime = meta_extract_start_timestamp(meta)
            except Exception:
                pass

            if starttime is None or starttime - nowtime < 300:
                # Rescrape next time we check handlers, at least 60 seconds later
                video.next_event_check = nowtime + 60
            else:
                # If more than two hours out, split time in half. Else, do 15min intervals.
                next_check = (starttime - nowtime) / 2 + starttime
                if starttime - next_check < 60 * 60 * 2:
                    next_check = nowtime + 60 * 15
                video.next_event_check = next_check

        def keep_func() -> bool:
            print('running keep func', video_id)
            meta = meta_load_fast(video_id)
            if meta is None:
                print('warning: keep func: fast meta load failed:', video_id)
            raw_status = meta_extract_raw_live_status(meta)
            # Will remove the handler when the video becomes live; we might change this behavior
            if raw_status == 'is_upcoming':
                return True

            if raw_status == 'is_live':
                print('not keeping anymore (now live)')
                return False

            print('not keeping anymore (not upcoming or is unscrapeable)')
            return False

        handler = {'handler_id': handler_id, 'time_func': time_func, 'keep_func': keep_func}

        handlers = context.events.get(video_id)
        if handlers is not None:
            reinstall = False
            for i in range(len(handlers)):
                h = handlers[i]
                if h['handler_id'] == handler_id:
                    print('warning: reinstalling existing with new handler:', handler_id, file=sys.stderr)
                    handlers[i] = handler
                    reinstall = True
                    break

            if not reinstall:
                handlers.append(handler)

            context.lives[video_id].next_event_check = get_timestamp_now()

        else:
            context.events[video_id] = [handler]
            context.lives[video_id].next_event_check = get_timestamp_now()

    except Exception:
        print('warning: failed to schedule rescrape', file=sys.stderr)


def run_periodic_rescrape_handler(video_id, context: AutoScraper):
    events = context.events
    handler_id = 'periodic_rescrape'
    time_now = get_timestamp_now()
    video = context.lives[video_id]
    if video.next_event_check > time_now:
        return

    handler = typing.cast(Dict[str, Callable], None)
    index = -1
    for i in range(len(events[video_id])):
        h = events[video_id][i]
        if h['handler_id'] == handler_id:
            handler = h
            index = i
            break

    time_func, keep_func = handler['time_func'], handler['keep_func']
    next_check = time_func()

    # scrape here
    rescrape_chatdownloader(video)
    persist_meta(video, context=context, fresh=True, clobber=True)

    video.next_event_check = next_check
    if not keep_func():
        del events[video_id][index]
        video.next_event_check = 0
        return


q: mp.SimpleQueue = mp.SimpleQueue()
statuslog: IO = typing.cast(IO, None)
mainpid: int = typing.cast(int, None)
altpid: int = typing.cast(int, None)


def process_dlpid_queue(*, context: AutoScraper):
    """ Process (empty) the queue of PIDs from newly invoked downloaders and update their state. """
    lives = context.lives

    while not q.empty():
        (pid, dlpid, vid) = q.get()

        try:
            lives[vid].set_progress('downloading')
        except TransitionException:
            if lives[vid].progress in {'unscraped', 'waiting', 'downloading'}:
                print(f"warning: discarding weird progress status {lives[vid].progress}, setting to downloading:", vid)
                lives[vid].reset_progress()
                lives[vid].set_progress('waiting')
                lives[vid].set_progress('downloading')

        context.pids[vid] = (pid, dlpid)
        persist_basic_state(lives[vid], context=context, clobber=False, clobber_pid=True)


def invoke_downloader(video: Video, *, context: AutoScraper):
    try:
        video_id = video.video_id

        print('invoking for ' + str(video_id))
        if video.progress == 'unscraped':
            print("warning: progress never set to 'waiting' for video: " + video_id + f" (status: {video.status}) (progress: {video.progress})", file=sys.stderr)

        if context.pids.get(video_id):
            (pypid, dlpid) = context.pids[video_id]
            pypid_ok = check_pid(pypid)
            dlpid_ok = check_pid(dlpid)
            print("warning: duplicate invocation for video " + video_id + f" (according to internal PID state. alive? pypid: {pypid}, {pypid_ok}; dlpid: {dlpid}, {dlpid_ok})", file=sys.stderr)
            if pypid_ok and dlpid_ok:
                print("warning:   cancelling invocation for video " + video_id + " (both pypid and dlpid present). status: {video.status}; progress: {video.progress}", file=sys.stderr)
                if video.progress == 'waiting':
                    video.set_progress('downloading')
                return

        if video.status not in {'prelive', 'live'}:
            print("warning: cancelling invocation for video " + video_id + f" (cannot invoke for status: {video.status})", file=sys.stderr)
            # HACK to stop the spam
            video.progress = 'missed'
            return

        nowtime = dt.datetime.utcnow()
        outfile = "_" + video_id + "_curr-" + str(nowtime.timestamp())

        title = video.meta.get('title')
        uploader = video.meta.get('uploader')
        channel_id = video.meta.get('channel_id')
        starttime = video.meta.get('live_starttime')
        live_status = video.status
        currtimesafe = safen_path(nowtime.isoformat(timespec='seconds')) + "_UTC"

        with open("by-video-id/" + video_id + ".loginfo", "a") as fp:
            res = {"video_id": video_id, "title": title, "channel_id": channel_id, "uploader": uploader, "starttime": starttime, "currtime": currtimesafe, "live_status": live_status, "basename": outfile}
            fp.write(json.dumps(res, indent=2))

        p = mp.Process(target=_invoke_downloader_start, args=(q, video_id, outfile))
        p.start()

        # Wait for the process to spawn and for the downloader PID to be sent.
        time.sleep(0.5)
        process_dlpid_queue(context=context)   # hopefully just what we just spawned

    except Exception:
        print("warning: downloader invocation failed because of an exception. printing traceback...", file=sys.stderr)
        traceback.print_exc()


def start_watchdog():
    """ Ensure the program exits after a top-level exception. """
    subprocess.run('date')
    subprocess.Popen([watchdogprog, str(os.getpid())])


def _invoke_downloader_start(q, video_id, outfile):
    # There is not much use for the python pid, we store the process ID only for debugging
    pid = os.getpid()
    print("process fork " + str(pid) + " is live, with outfile " + outfile)
    proc = subprocess.Popen([downloadchatprgm, outfile, video_id])

    q.put((pid, proc.pid, video_id))
    # Close the queue to flush it and avoid blocking the python process on exit.
    time.sleep(0.1)
    try:
        q.close()
    except AttributeError:
        pass  # older python versions (pre-3.9) lack close()
    # Block this fork (hopefully not the main process)
    try:
        proc.wait()
        print("process fork " + str(pid) + " has waited (video: " + video_id + ")")
    except KeyboardInterrupt:
        print("process fork " + str(pid) + " was interrupted (video: " + video_id + ")")
        raise KeyboardInterrupt from None


def delete_ytmeta_raw(video: Video, *, context: AutoScraper = None, suffix: str = None):
    """ Delete ytmeta['raw'] field that eats memory; count deletions """
    general_stats = getattr(context, 'general_stats', {})
    try:
        video.rawmeta = None
        keyname = 'ytmeta del successes'
        if suffix:
            keyname = keyname + suffix
        general_stats[keyname] = general_stats.setdefault(keyname, 0) + 1
    except (KeyError, AttributeError):
        keyname = 'ytmeta del failures'
        if suffix:
            keyname = keyname + suffix
        general_stats[keyname] = general_stats.setdefault(keyname, 0) + 1


def _get_status_log():
    return statuslog


def process_one_status(video: Video, *, context: AutoScraper, first=False, just_invoked=False, force=False):
    # Process only on change
    if video.did_progress_print:
        if not force:
            return
        else:
            print(f'forced progress update for video {video.video_id}')
    else:
        print(f'progress update for video {video.video_id}, reason: {video.progress_flush_reason}')
        video.did_progress_print = True

    video_id = video.video_id
    started_download = False

    statuslog = _get_status_log()
    if statuslog is None:
        statuslog = sys.stdout

    if video.progress == 'waiting':
        if video.meta is None:
            print("error: video.meta missing for video " + video_id, file=sys.stderr)
            # video.prepare_meta()
        else:
            print("status: just invoked: " + video_id, file=statuslog)
            invoke_downloader(video, context=context)
            started_download = True

    elif video.progress == 'missed':
        if first:
            print("status: missed (possibly cached?): " + video_id, file=statuslog)
        else:
            print("status: missed: " + video_id, file=statuslog)

        delete_ytmeta_raw(video, context=context)

    elif video.progress == 'invalid':
        if first:
            print("status: upload (possibly cached/bogus?): " + video_id, file=statuslog)
        else:
            print("status: upload: " + video_id, file=statuslog)

        delete_ytmeta_raw(video, context=context)

    elif video.progress == 'aborted':
        if first:
            print("status: aborted (possibly cached/bogus?): " + video_id, file=statuslog)
        else:
            print("status: aborted: " + video_id, file=statuslog)

        delete_ytmeta_raw(video, context=context)

    elif video.progress == 'downloading':
        if first:
            print("status: downloading (but this is wrong; we just started!): " + video_id, file=statuslog)

        wants_rescrape = False

        if context.pids.get(video_id):
            (pypid, dlpid) = context.pids[video_id]

            if not check_pid(dlpid):
                print("status: dlpid no longer exists: " + video_id, file=statuslog)

                # Check before making this video unredownloadable
                wants_rescrape = True

            else:
                if first:
                    print("status: downloading (apparently, may be bogus): " + video_id, file=statuslog)
                else:
                    print("status: downloading: " + video_id, file=statuslog)

        else:
            if first:
                print("warning: pid lookup for video " + video_id + " failed (initial load, should be unreachable).", file=sys.stderr)
            else:
                print("warning: pid lookup for video " + video_id + " failed.", file=sys.stderr)

            print("status: unknown: " + video_id, file=statuslog)

            wants_rescrape = True

        if wants_rescrape and just_invoked:
            print("warning: invocation seems unsuccessful, avoiding immediate retry for video: " + video.video_id, file=sys.stderr)

        elif wants_rescrape:
            # Check status
            downloader = ChatDownloader()
            youtube = downloader.create_session(YouTubeChatDownloader)

            details = None
            try:
                details = youtube.get_video_data(video_id, params={'max_attempts': 3})
            except Exception:
                pass

            if details and details.get('status') in {'live', 'upcoming'}:
                print("warning: downloader seems to have exited prematurely. reinvoking:", video_id, file=sys.stderr)

                try:
                    # assume process is dead so that we can track the new one without issue
                    del context.pids[video_id]
                except KeyError:
                    pass

                invoke_downloader(video, context=context)
                started_download = True

            else:
                print("downloader complete:", video_id, file=sys.stderr)
                video.set_progress('downloaded')
                video.set_status('postlive')  # a safe assumption

                try:
                    del context.pids[video_id]
                except KeyError:
                    pass

                persist_basic_state(video, context=context)
                delete_ytmeta_raw(video, context=context)

    elif video.progress == 'downloaded':
        if first:
            print("status: finished (cached?): " + video_id, file=statuslog)
        else:
            print("status: finished: " + video_id, file=statuslog)

            delete_ytmeta_raw(video, context=context)

    else:
        print("warning: new downloader status is weird state '{video.progress}': " + video_id, file=statuslog)

    if not video.did_meta_flush:
        print("warning: didn't flush meta for video; flushing now", file=sys.stderr)
        persist_ytmeta(video, fresh=True)

    if started_download:
        if video.did_progress_print:
            print("warning: didn't get dlpid in a timely manner: " + video.video_id, file=sys.stderr)
        else:
            process_one_status(video, context=context, first=False, just_invoked=True)
            persist_basic_state(video, context=context)
            if video.progress == 'downloading':
                delete_ytmeta_raw(video, context=context)

    statuslog.flush()


def dump_lives(context: AutoScraper):
    with open("dump/lives", "w") as fp:
        for video in context.lives.values():
            # Fine as long as no objects in the class.
            fp.write(json.dumps(video.__dict__, sort_keys=True))


def dump_pids(context: AutoScraper):
    with open("dump/pids", "w") as fp:
        fp.write(json.dumps(context.pids))


def dump_misc(context: AutoScraper):
    with open("dump/general_stats", "w") as fp:
        fp.write(json.dumps(context.general_stats))

    with open("dump/staticconfig", "w") as fp:
        print("FORCE_RESCRAPE=" + str(FORCE_RESCRAPE), file=fp)
        print("DISABLE_PERSISTENCE=" + str(DISABLE_PERSISTENCE), file=fp)
        print("PERIODIC_SCRAPES=" + str(PERIODIC_SCRAPES), file=fp)
        print("SCRAPER_SLEEP_INTERVAL=" + str(SCRAPER_SLEEP_INTERVAL), file=fp)
        print("CHANNEL_SCRAPE_LIMIT=" + str(CHANNEL_SCRAPE_LIMIT), file=fp)


def handle_debug_signal(signum, frame):
    if os.getpid() != mainpid:
        print('warning: got debug signal, but mainpid doesn\'t match', file=sys.stderr)
        return

    os.makedirs('dump', exist_ok=True)

    try:
        dump_lives(context=main_autoscraper)
    except Exception:
        print('debug: dumping lives failed.')
        traceback.print_exc()
    else:
        print('debug: dumping lives succeeded.')

    dump_pids(context=main_autoscraper)
    dump_misc(context=main_autoscraper)


def handle_special_signal(signum, frame):
    global mainpid
    if os.getpid() != mainpid:
        print('warning: got reexec signal, but mainpid doesn\'t match', file=sys.stderr)
        return

    statuslog.close()
    os.makedirs('dump', exist_ok=True)

    try:
        dump_lives(context=main_autoscraper)
    except Exception:
        print('reexec: dumping lives failed. will restart...')
        traceback.print_exc()
        restart()
    else:
        print('reexec: dumping lives succeeded.')

    dump_pids(context=main_autoscraper)
    dump_misc(context=main_autoscraper)

    print('reexec: about to start')
    reexec()


def load_dump():
    print('reexec: reexec specified, loading dump')
    if not os.path.exists('dump'):
        os.chdir('oo')
    if not os.path.exists('dump'):
        print('reexec: cannot load from dump; dump directory not found')
        return False

    try:
        os.system('jq -as <dump/lives >dump/lives.jq')
        with open("dump/lives.jq", "r") as fp:
            jsonres = json.load(fp)
            for viddict in jsonres:
                video = Video('XXXXXXXXXXX')
                video.__dict__ = viddict
                main_autoscraper.lives[video.video_id] = video
    except Exception:
        print('reexec: recalling lives failed.')
        main_autoscraper.lives = {}
        traceback.print_exc()
        return False
    else:
        print('reexec: recalling lives succeeded.')

    try:
        with open("dump/pids", "r") as fp:
            jsonres = json.load(fp)
            main_autoscraper.pids = jsonres
            print("reexec: number of videos loaded from pids: " + str(len(main_autoscraper.pids)))
            for video_id in main_autoscraper.pids:
                (pypid, dlpid) = main_autoscraper.pids[video_id]

                if not check_pid(dlpid):
                    print("reexec: warning: dlpid no longer exists: " + video_id)
    except Exception:
        print('reexec: recalling pids failed.')
        traceback.print_exc()
    else:
        print('reexec: recalling pids succeeded.')

    return True


def restart():
    global mainpid
    os.chdir('..')
    print(f"{mainpid = }, going away for program restart")
    os.execl('./scraper_oo.py', './scraper_oo.py')


def reexec():
    global mainpid
    os.chdir('..')
    print(f"{mainpid = }, going away for program reexec")
    os.execl('./scraper_oo.py', './scraper_oo.py', 'reexec')


rescrape = rescrape_ytdlp

invoke_scraper = invoke_scraper_ytdlp


def start_alt_main():
    """ Create alt-main process """
    global altpid
    alt_proc = mp.Process(target=_invoke_alt_main)
    alt_proc.start()
    altpid = alt_proc.pid
    print(f"{altpid = }")


def _invoke_alt_main():
    """ Process target for starting alt-main """
    global altpid
    global is_true_main
    global main_autoscraper
    altpid = os.getpid()
    is_true_main = False
    main_autoscraper = AutoScraper()

    print("alternate main loop process " + str(altpid) + " started.")
    alt_main(main_autoscraper)


def alt_main(context: AutoScraper):
    """ Dedicated scraper for cookied scrapes/downloads
        Ideal for member content or other cookied high-priority content.
        Conflicts should be avoided by the user, for now.
    """
    print("Updating lives status (alt-main)", flush=True)
    # Alt-main hacks here
    context.update_lives_status()

    if True:
        # Initial load
        print("Starting alt-main initial pass", flush=True)

        try:
            main_initial_scrape_task(context=context)

        except KeyboardInterrupt:
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during initial load processing (alt main)") from exc

    print("Starting alt-main loop", flush=True)
    while True:
        try:
            time.sleep(SCRAPER_SLEEP_INTERVAL)

            main_scrape_task(context=context)

        except KeyboardInterrupt:
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during main loop processing (alt main)") from exc

        finally:
            print_autoscraper_statistics(context=context)


def main(context: AutoScraper):
    global mainpid
    mainpid = os.getpid()
    print(f"{mainpid = }")

    fast_startup = False
    if len(sys.argv) == 2 and sys.argv[1] == 'reexec':
        print("reexec: number of inherited children: " + str(len(mp.active_children())))   # side effect: joins finished tasks -- exec doesn't seem to inherit children
        fast_startup = load_dump()

    if not fast_startup:
        # Prep storage and persistent state directories
        os.makedirs('oo', exist_ok=True)
        os.chdir('oo')
        os.makedirs('by-video-id', exist_ok=True)
        os.makedirs('chat-logs', exist_ok=True)
        os.makedirs('pid', exist_ok=True)

    signal.signal(signal.SIGUSR1, handle_special_signal)
    signal.signal(signal.SIGUSR2, handle_debug_signal)

    # For cookied downloads
    if ENABLE_ALTMAIN:
        start_alt_main()
    write_cgroup(mainpid)

    print("Updating lives status", flush=True)
    context.update_lives_status()

    nowtimestamp = str(get_timestamp_now())
    with open("discovery.txt", "a") as dlog:
        print("program started: " + nowtimestamp, file=dlog, flush=True)
        dlog.flush()
    global statuslog
    statuslog = open("status.txt", "a")
    print("program started: " + nowtimestamp, file=statuslog)
    statuslog.flush()
    os.fsync(statuslog.fileno())

    if not fast_startup:
        # Initial load
        print("Starting initial pass", flush=True)

        try:
            main_initial_scrape_task(context=context)

        except KeyboardInterrupt:
            statuslog.flush()
            os.fsync(statuslog.fileno())
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during initial load processing") from exc

    else:
        print("Skipped initial pass; doing simple corruption check.", flush=True)
        main_reexec_corruption_check(context=context)

    statuslog.flush()

    print("Starting main loop", flush=True)
    while True:
        try:
            if fast_startup:
                fast_startup = False
                print("reducing initial loop delay", flush=True)
                time.sleep(5)
            else:
                time.sleep(SCRAPER_SLEEP_INTERVAL)

            main_scrape_task(context=context)

        except KeyError:
            print("warning: internal inconsistency! squashing KeyError exception...", file=sys.stderr)

        except KeyboardInterrupt:
            statuslog.flush()
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during main loop processing") from exc

        finally:
            print_autoscraper_statistics(context=context)

            statuslog.flush()


def main_reexec_corruption_check(*, context):
    """ Reexec-specific corruption/crash check """
    for video in context.lives.values():
        if video.progress == 'waiting':
            print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash: {video.progress} -> unscraped")
            video.reset_progress()

        elif video.progress == 'downloading':
            try:
                (pypid, dlpid) = context.pids[video.video_id]
                if not check_pid(dlpid):
                    # if the OS recycles PIDs, then this check might give bogus results. Obviously, don't 'reexec' after an OS reboot.
                    dlinfofile = f'by-video-id/{video.video_id}.dlend'
                    crashed = True
                    if os.path.exists(dlinfofile):
                        try:
                            dlinfo = json.load(open(dlinfofile))
                            crashed = dlinfo.get('exit_cause') != 'finished'
                        except json.JSONDecodeError:
                            try:
                                dlinforaw = open(dlinfofile).read()
                                for dlinfo in json_stream_wrapper(dlinforaw):
                                    crashed = dlinfo.get('exit_cause') != 'finished'
                                    if not crashed:
                                        break
                            except json.JSONDecodeError:
                                pass

                    if crashed:
                        print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash (pid {dlpid}: check failed): {video.progress} -> unscraped")
                        video.reset_progress()

            except KeyError:
                print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash (pid unknown!): {video.progress} -> unscraped")
                video.reset_progress()

            except Exception:
                print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash (exception!): {video.progress} -> unscraped")
                video.reset_progress()


def main_initial_scrape_task(*, context):
    """ Task for each iteration of the main loop, without added delay. """
    # Populate cache from disk
    for video_id, video in context.lives.items():
        progress = video.progress

        if progress == 'unscraped':
            # Try to load missing meta from disk
            recall_video(video_id, context=context)

    # There is a 4-hour explanation for this line, take a guess what happened.
    del video_id, video

    # Try to make sure downloaders are tracked with correct state
    process_dlpid_queue(context=context)

    # Scrape each video again if needed
    for video in context.lives.values():
        maybe_rescrape_initially(video, context=context)

    for video in context.lives.values():
        process_one_status(video, context=context, first=True)


def main_scrape_task(*, context):
    """ Task run before the main loop starts"""
    context.update_lives_status()

    # Try to make sure downloaders are tracked with correct state
    process_dlpid_queue(context=context)

    # Scrape each video again if needed
    for video in context.lives.values():
        maybe_rescrape(video, context=context)

    for video_id in context.pids.copy():
        if video not in context.lives:
            recall_video(video_id, context=context, filter_progress=True)
            if video.progress == 'waiting':
                # This may modify our pid list, take care above.
                try:
                    (pypid, dlpid) = context.pids[video.video_id]
                    if not check_pid(dlpid):
                        process_one_status(video, context=context, force=True)

                except KeyError:
                    process_one_status(video, context=context, force=True)

    for video in context.lives.values():
        # while there is a duplication of effort here, we need to advance the progress once somehow...
        if not video.did_progress_print:
            process_one_status(video, context=context)
        elif video.progress == 'downloading':
            try:
                (pypid, dlpid) = context.pids[video.video_id]
                if not check_pid(dlpid):
                    process_one_status(video, context=context, force=True)

            except KeyError:
                print(f"(loop check) video {video.video_id}: resetting progress after possible corruption (pid unknown!): {video.progress} -> unscraped")
                video.reset_progress()


def print_autoscraper_statistics(*, context: AutoScraper):
    print("number of active children: " + str(len(mp.active_children())))   # side effect: joins finished tasks
    print("number of known lives: " + str(len(context.lives)))

    counters: Dict[str, Any] = {'progress': {}, 'status': {}, 'meta': 0, 'rawmeta': 0}
    for video in context.lives.values():
        counters['status'][video.status] = counters['status'].setdefault(video.status, 0) + 1
        counters['progress'][video.progress] = counters['progress'].setdefault(video.progress, 0) + 1
        counters['meta'] += (video.meta is not None)
        counters['rawmeta'] += (video.rawmeta is not None)

    print("video states:")
    for status, count in counters['status'].items():
        print(f"  number with video state {status}:", count)

    print("progress states:")
    for progress, count in counters['progress'].items():
        print(f"  number with progress state {progress}:", count)

    print("number of meta objects:", counters['meta'])
    print("number of rawmeta objects:", counters['rawmeta'])
    print("number of tracked pid groups: " + str(len(context.pids)))

    pid_count = 0
    for video_id in context.pids:
        (pypid, dlpid) = context.pids[video_id]

        if check_pid(dlpid):
            pid_count += 1

    print("number of valid tracked pids: " + str(pid_count))
    print(end='', flush=True)


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(0)
    main(context=main_autoscraper)
