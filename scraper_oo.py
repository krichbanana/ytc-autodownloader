#!/usr/bin/env python3
from bs4 import BeautifulSoup
import os
import subprocess
import sys
import json
import multiprocessing as mp
import datetime as dt
import time
import traceback
import signal
from chat_downloader import ChatDownloader
from chat_downloader.sites import YouTubeChatDownloader

from utils import extract_video_id_from_yturl
try:
    from write_cgroup import write_cgroup
except ImportError:
    def write_cgroup(mainpid):
        pass


# Debug switch
DISABLE_PERSISTENCE = False
FORCE_RESCRAPE = False
SCRAPER_SLEEP_INTERVAL = 120 * 5 / 2
CHANNEL_SCRAPE_LIMIT = 30

downloadmetacmd = "../yt-dlp/yt-dlp.sh -s -q -j --ignore-no-formats-error "
downloadchatprgm = "../downloader.py"
channelscrapecmd = "../scrape_channel_oo.sh"
channelpostscrapecmd = "../scrape_community_tab.sh"
channelsfile = "./channels.txt"
watchdogprog = "../watchdog.sh"
holoscrapecmd = 'wget -nv --load-cookies=../cookies-schedule-hololive-tv.txt https://schedule.hololive.tv/lives -O auto-lives_tz'

# dict: video_id => Video
lives = {}
channels = {}
pids = {}
general_stats = {}  # for debugging

statuses = {'unknown', 'prelive', 'live', 'postlive', 'upload'}
progress_statuses = {'unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'}


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
    def __init__(self, video_id):
        self.video_id = video_id
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
        self.did_status_print = False
        self.did_progress_print = False
        self.did_discovery_print = False
        self.did_meta_flush = False

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

        if status == self.status:
            print(f"warning: new video status suspicious: no change in status: {status}", file=sys.stderr)
            self.warned = True
        else:
            self.did_status_print = False
            self.did_meta_flush = False

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

        if progress == self.progress:
            print(f"warning: new progress status suspicious: no change in progress: {progress}", file=sys.stderr)
            self.warned = True
        else:
            self.did_progress_print = False
            self.did_meta_flush = False

        self.transition_timestamp = get_timestamp_now()
        self.progress = progress

    def reset_status(self):
        """ Set the status to 'unknown'. Useful for clearing state loaded from disk. """
        self.status = 'unknown'

    def reset_progress(self):
        """ Set progress to 'unscraped'. Useful for clearing state loaded from disk. """
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
        lastmeta = self.meta
        self.meta = None

        try:
            rescrape(self)
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
        new_videos = set(video_id) - self.videos
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


def update_lives():
    subprocess.run(holoscrapecmd, shell=True)

    html_doc = ''
    with open("auto-lives_tz", "rb") as fp:
        html_doc = fp.read()

    soup = BeautifulSoup(html_doc, 'html.parser')
    with open("auto-lives_tz", "wb") as fp:
        fp.write(soup.prettify().encode())

    return soup


def update_lives_status():
    with open("discovery.txt", "a") as dlog:
        try:
            update_lives_status_holoschedule(dlog)
        except Exception:
            print("warning: exception during holoschedule scrape. Network error?")
            traceback.print_exc()

        try:
            update_lives_status_urllist(dlog)
        except Exception:
            print("warning: exception during urllist scrape. Network error?")
            traceback.print_exc()

        try:
            update_lives_status_channellist(dlog)
        except Exception:
            print("warning: exception during channellist scrape. Network error?")
            traceback.print_exc()


def update_lives_status_holoschedule(dlog):
    # Find all sections indicated by a 'day' header
    soup = update_lives()
    allcont = soup.find(id='all')
    allcontchildren = [node for node in allcont.children if len(repr(node)) > 4]

    localdate = ''
    newlives = 0
    knownlives = 0

    error = False

    try:
        for child in allcontchildren:
            day = child.find(class_='navbar-text')

            if day:
                localdate = [x for x in day.stripped_strings][0].split()[0]

            # Extract MM/DD date from header
            for link in child.find_all('a'):
                # Process Youtube link; get HH:MM starttime and user-friendly channel name (not localized) if possible
                items = None
                localtime = ''
                channelowner = ''
                video_id = None
                malformed = False

                # Extract link
                href = link.get('href')

                video_id = extract_video_id_from_yturl(href)

                if video_id is None:
                    error = True

                    continue

                # Check for existing state
                if video_id not in lives:
                    recall_video(video_id, filter_progress=True)
                    video = lives[video_id]

                if video_id not in lives:
                    video = Video(video_id)
                    lives[video_id] = video

                    try:
                        items = [x for x in link.stripped_strings]
                        localtime = items[0]
                        channelowner = items[1]
                    except Exception:
                        malformed = True

                    if not malformed:
                        print("discovery: new live listed: " + video_id + " " + localdate + " " + localtime + " : " + channelowner, file=dlog, flush=True)
                    else:
                        print("discovery: new live listed (malformed page): " + video_id, file=dlog, flush=True)

                    newlives += 1
                else:
                    # known (not new) live listed
                    knownlives += 1

    except Exception:
        error = True
        traceback.print_exc()

    if newlives + knownlives == 0 or error:
        print("warning: unexpected error when processing holoschedule page (found " + str(newlives + knownlives) + " total lives), using fallback", file=sys.stderr)
        saved_newlives = newlives
        newlives = 0
        knownlives = 0
        error = False

        for link in soup.find_all('a'):
            # Extract any link
            href = link.get('href')
            video_id = None

            video_id = extract_video_id_from_yturl(href)

            if video_id is None:
                error = True

                continue

            if not malformed:
                if video_id not in lives:
                    recall_video(video_id, filter_progress=True)
                    video = lives[video_id]

                if video_id not in lives:
                    video = Video(video_id)
                    lives[video_id] = video
                    print("discovery: new live listed (fallback extraction): " + video_id, file=dlog, flush=True)
                    newlives += 1
                else:
                    # known (not new) live listed
                    knownlives += 1

        print("discovery: holoschedule: (fallback) new lives: " + str(newlives))
        print("discovery: holoschedule: (fallback) new lives (initial try): " + str(saved_newlives))
        print("discovery: holoschedule: (fallback) known lives: " + str(knownlives))

        if error:
            print("note: video id extraction errors occured when processing holoschedule page using fallback method (found " + str(newlives + knownlives) + " total lives)", file=sys.stderr)

    else:
        print("discovery: holoschedule: new lives: " + str(newlives))
        print("discovery: holoschedule: known lives: " + str(knownlives))


def update_lives_status_urllist(dlog):
    # TODO
    pass


def update_lives_status_channellist(dlog):
    """ Read channels.txt for a list of channel IDs to process. """
    try:
        if os.path.exists(channelsfile):
            with open(channelsfile) as channellist:
                for channel_id in [x.strip().split()[0] for x in channellist.readlines()]:
                    channel = None
                    use_ytdlp = False

                    if channel_id in channels:
                        channel = channels[channel_id]
                    else:
                        channel = Channel(channel_id)
                        channels[channel_id] = channel
                        # use chat_downloader to get initial video list
                        print("New channel: " + channel.channel_id)

                    if not use_ytdlp:
                        try:
                            scrape_and_process_channel_chatdownloader(channel, dlog)
                        except Exception:
                            print("failed to scrape channel list with chat_downloader:", channel_id, file=sys.stderr)
                            traceback.print_exc()
                            use_ytdlp = True

                    if use_ytdlp:
                        invoke_channel_scraper(channel)
                        process_channel_videos(channel, dlog)

                    # Scrape community tab page for links (esp. member stream links)
                    # Currently only try this when cookies are provided.
                    if os.path.exists(channel_id + ".txt"):
                        invoke_channel_scraper(channel, community_scrape=True)
                        process_channel_videos(channel, dlog)

    except Exception:
        print("warning: unexpected error with processing channels.txt", file=sys.stderr)
        traceback.print_exc()


def rescrape_chatdownloader(video: Video, channel=None, youtube=None):
    """ rescrape_ytdlp, but using chat_downloader """
    video_id = video.video_id
    video_data, player_response, status = invoke_scraper_chatdownloader(video_id, youtube)
    microformat = player_response['microformat']['playerMicroformatRenderer']
    video_details = player_response['videoDetails']

    # keep only known useful fields, junk spam/useless fields
    old_player_response = player_response
    player_response = {}
    for key in ['playabilityStatus', 'videoDetails', 'microformat']:
        player_response[key] = old_player_response[key]
    for key in ['streamingData']:
        player_response[key] = old_player_response.get(key)
    del old_player_response

    # "export" the fields manually here
    meta = {}

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

    video.set_status(status)
    video.reset_progress()
    video.meta = meta
    rawmeta = meta.get('raw')
    if not rawmeta:
        # Note: rawmeta may be older than meta, but it's better than being lost.
        video.set_progress('aborted')
    else:
        video.rawmeta = rawmeta
        video.set_progress('waiting')
    video.meta_timestamp = get_timestamp_now()

    try:
        del meta['raw']
    except KeyError:
        pass


def invoke_scraper_chatdownloader(video_id, youtube=None, skip_status=False):
    """ Like invoke_scraper_ytdlp, but use chat_downloader's python interface instead of forking and calling yt-dlp.
        Try to export the status for the autoscraper as well.
        Returns raw YouTube data and the deduced status.
    """
    if youtube is None:
        downloader = ChatDownloader()
        youtube = downloader.create_session(YouTubeChatDownloader)

    video_data, player_response, *_ = youtube._parse_video_data(video_id)

    scraper_status = None
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
            scraper_status = 'unknown'

    return video_data, player_response, scraper_status


def scrape_and_process_channel_chatdownloader(channel: Channel, dlog):
    """ Use chat_downloader's get_user_videos() to quickly get channel videos and live statuses. """

    downloader = ChatDownloader()

    # Forcefully create a YouTube session
    youtube = downloader.create_session(YouTubeChatDownloader)

    limit = CHANNEL_SCRAPE_LIMIT
    count = 0
    perpage_count = 0
    valid_count = 0
    skipped = 0

    seen_vids = set()

    # We don't just check 'all' since the list used may be slow to update.
    for video_status in ['upcoming', 'live', 'all']:
        perpage_count = 0
        time.sleep(0.1)
        for basic_video_details in youtube.get_user_videos(channel_id=channel.channel_id, video_status=video_status):
            status = 'unknown'
            status_hint = None

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
                if video_id is not None and lives[video_id].progress not in {'unscraped', 'aborted'} and lives[video_id].status not in {'postlive', 'upload'}:
                    print(f"warning: status hint extraction: unexpected KeyError... {count = } {perpage_count = } (+1) ... {valid_count = } {skipped = } {limit = } ... {seen_vids = } ... {basic_video_details = })", file=sys.stderr)
                    traceback.print_exc()
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

            if video_id not in lives:
                lives[video_id] = Video(video_id)

            video = lives[video_id]

            channel.add_video(video)

            rescrape_chatdownloader(video, channel=channel, youtube=youtube)

            persist_meta(video, fresh=True)

            if perpage_count >= limit:
                print(f"perpage limit of {limit} reached:", video_status)
                break

        if count >= limit * 3:
            print(f"limit of {limit} reached")
            break

    print(f"discovery: channels list (via chat_downloader): channel {channel.channel_id} new upcoming/live lives: " + str(valid_count) + "/" + str(count) + " (" + str(skipped) + " known)")


def invoke_channel_scraper(channel: Channel, community_scrape=False):
    """ Scrape the channel for latest videos and batch-fetch meta state. """
    # Note: some arbitrary limits are set in the helper program that may need tweaking.
    if not community_scrape:
        print("Scraping channel " + channel.channel_id)
        subprocess.run(channelscrapecmd + " " + channel.channel_id, shell=True)
    else:
        print("Scraping channel community pages " + channel.channel_id)
        subprocess.run(channelpostscrapecmd + " " + channel.channel_id, shell=True)

    with open("channel-cached/" + channel.channel_id + ".meta.new") as allmeta:
        metalist = []

        for jsonres in allmeta.readlines():
            try:
                metalist.append(export_scraped_fields_ytdlp(json.loads(jsonres)))
            except Exception:
                if community_scrape:
                    print("warning: exception in channel post scrape task (corrupt meta?)", file=sys.stderr)
                else:
                    print("warning: exception in channel scrape task (corrupt meta?)", file=sys.stderr)
                traceback.print_exc()

        for ytmeta in metalist:
            video_id = ytmeta["id"]
            recall_video(video_id, filter_progress=True)
            video = lives.get(video_id)
            if video and video.meta is None:
                video.meta = ytmeta
                video.rawmeta = ytmeta.get('raw')
                video.did_meta_flush = False
            else:
                if community_scrape:
                    print("ignoring ytmeta from channel post scrape")
                else:
                    print("ignoring ytmeta from channel scrape")


# TODO: rewrite
def process_channel_videos(channel: Channel, dlog):
    """ Read scraped channel video list, proccess each video ID, and persist the meta state. """
    newlives = 0
    knownlives = 0
    numignores = {}
    channel_id = channel.channel_id
    channel.did_discovery_print = True

    channel.start_batch()

    try:
        with open("channel-cached/" + channel_id + ".url.all") as urls:
            for video_id in [f.split(" ")[1].strip() for f in urls.readlines()]:
                # Process each recent video
                if video_id not in lives:
                    recall_video(video_id, filter_progress=True)

                video = lives[video_id]
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

                    delete_ytmeta_raw(video_id, suffix=" (channel)")

                    continue

                cache_miss = False

                # process precached meta
                if video.meta is None:
                    # We may be reloading old URLs after a program restart
                    print("ytmeta cache miss for video " + video_id + " on channel " + channel_id)
                    cache_miss = True
                    rescrape(video)
                    if video.meta is None:
                        # scrape failed
                        continue
                    video.rawmeta = video.meta.get('raw')
                    video.did_meta_flush = False

                process_ytmeta(video)

                # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
                # I look at this and am confused by the '==' here (and one place elsewhere)...
                if cache_miss or (saved_progress not in {'missed', 'invalid'} and saved_progress != video.progress):
                    persist_meta(video, fresh=True)

                if not video.did_meta_flush:
                    # Essentially nerfs the above performance optimization...
                    print("warning: didn't flush meta for channel video; flushing now", file=sys.stderr)
                    persist_meta(video, fresh=True)

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


def persist_meta(video: Video, fresh=False, clobber=True):
    video_id = video.video_id

    metafile = 'by-video-id/' + video_id

    # Debug switch
    if DISABLE_PERSISTENCE:
        print('NOT updating ' + metafile)
        return

    if clobber or not os.path.exists(metafile):
        print('Updating ' + metafile)

    pidfile = 'pid/' + video_id
    meta = {}
    meta['status'] = video.status

    # TODO: only process_dlpid_queue uses fresh=False, so the "saved" progress is mostly useless.
    # Best just special-case that setter function, if even needed.
    meta['progress'] = video.progress

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
                print('Updating ' + metafileyt_status)
                with open(metafileyt_status, 'wb') as fp:
                    fp.write(json.dumps(ytmeta, indent=1).encode())
        finally:
            try:
                # Since we don't deep-copy, don't keep 'raw' in the meta dict.
                if video.rawmeta is not None:
                    del video.meta['raw']
            except KeyError:
                pass

    if clobber or not os.path.exists(metafile):
        with open(metafile, 'wb') as fp:
            fp.write(json.dumps(meta, indent=1).encode())

    if clobber or not os.path.exists(pidfile):
        with open(pidfile, 'wb') as fp:
            if pids.get(video_id) is not None:
                # Write dlpid to file
                fp.write(str(pids[video_id][1]).encode())

    video.did_meta_flush = True


# TODO: replace recall_meta with recall_video
def recall_video(video_id: str, filter_progress=False):
    """ Read status, progress for video_id.
        If filter_progress is set to True, avoid ytmeta loads for certain progress states,
        unless unconditional rescraping is set.
    """
    # Not cached in memory, look for saved state.
    metafile = 'by-video-id/' + video_id
    metafileyt = metafile + ".meta"
    valid_meta = os.path.exists(metafile)
    valid_ytmeta = os.path.exists(metafileyt)
    meta = None
    ytmeta = None
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
        if filter_progress:
            should_ignore = meta['progress'] in {'downloaded', 'missed', 'invalid', 'aborted'}

        # note: FORCE_RESCRAPE might clobber old ytmeta if not loaded (bad if the video drastically changes or goes unavailable)
        if valid_ytmeta and not should_ignore:
            with open(metafileyt, 'rb') as fp:
                try:
                    ytmeta = json.loads(fp.read())
                    valid_ytmeta = 'ytmeta' in ytmeta

                except (json.decoder.JSONDecodeError, KeyError):
                    valid_ytmeta = False

    video = Video(video_id)
    lives[video_id] = video

    if valid_meta:
        # Commit status to runtime tracking (else we would discard it here)
        # Direct assignment here to avoid checks, might rewrite later
        video.status = meta['status']
        video.progress = meta['progress']

        if valid_ytmeta and not should_ignore:
            video.meta = ytmeta['ytmeta']
            video.rawmeta = ytmeta['ytmeta'].get('raw')
            if video.rawmeta is not None:
                del video.meta['raw']

        # unmigrated (monolithic file) format
        elif 'ytmeta' in meta:
            video.meta = meta['ytmeta']
            video.rawmeta = meta['ytmeta'].get('raw')
            if video.rawmeta is not None:
                del video.meta['raw']

            if DISABLE_PERSISTENCE:
                return

            print('notice: migrating ytmeta in status file to new file right now: ' + metafile)
            persist_meta(video, fresh=True)

            if should_ignore:
                delete_ytmeta_raw(video, suffix=" (meta recall)")


def process_ytmeta(video: Video):
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


def maybe_rescrape(video: Video):
    saved_progress = video.progress
    if video.progress == 'unscraped':
        video.rescrape_meta()
        if video.meta is None:
            # scrape failed
            return

        process_ytmeta(video)

        # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
        if saved_progress not in {'missed', 'invalid'} and saved_progress != video.progress:
            persist_meta(video, fresh=True)


def maybe_rescrape_initially(video: Video):
    if video.progress == 'downloading':
        video.reset_progress()

    if video.progress == 'unscraped' or FORCE_RESCRAPE:
        video.rescrape_meta()
        if video.meta is None:
            # scrape failed
            return

        process_ytmeta(video)

    # Redundant, but purges corruption
    persist_meta(video, fresh=True)


def export_scraped_fields_ytdlp(jsonres):
    ytmeta = {}
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


def rescrape_ytdlp(video: Video):
    """ Invoke the scraper, yt-dlp, on a video now.
        Sets a restructured json result as meta.
    """
    jsonres = invoke_scraper_ytdlp(video.video_id)
    if jsonres is None:
        # Mark as aborted here, before processing
        video.set_progress('aborted')

        return None

    video.meta = export_scraped_fields_ytdlp(jsonres)


def invoke_scraper_ytdlp(video_id):
    if video_id not in lives:
        raise ValueError('invalid video_id')

    proc = None
    try:
        cmdline = downloadmetacmd + "-- " + video_id
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


q = mp.SimpleQueue()


def process_dlpid_queue():
    """ Process (empty) the queue of PIDs from newly invoked downloaders and update their state. """
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

        pids[vid] = (pid, dlpid)
        persist_meta(lives[vid])


def invoke_downloader(video: Video):
    try:
        video_id = video.video_id

        print('invoking for ' + str(video_id))

        if pids.get(video_id):
            print("warning: duplicate invocation for video " + video_id + " (according to internal PID state)", file=sys.stderr)

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
        process_dlpid_queue()   # hopefully just what we just spawned

    except Exception:
        print("warning: downloader invocation failed because of an exception. printing traceback...", file=sys.stderr)
        traceback.print_exc()


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    else:
        return True


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
        print("process fork " + str(pid) + " has waited")
    except KeyboardInterrupt:
        print("process fork " + str(pid) + " was interrupted")
        raise KeyboardInterrupt from None


def delete_ytmeta_raw(video: Video, suffix=None):
    """ Delete ytmeta['raw'] field that eats memory; count deletions """
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


def process_one_status(video: Video, first=False):
    # Process only on change
    if video.did_status_print:
        return

    video_id = video.video_id

    if video.progress == 'waiting':
        if video.meta is None:
            print("error: video.meta missing for video " + video_id, file=sys.stderr)
            # video.prepare_meta()
        else:
            print("status: just invoked: " + video_id, file=statuslog)
            invoke_downloader(video)

    elif video.progress == 'missed':
        if first:
            print("status: missed (possibly cached?): " + video_id, file=statuslog)
        else:
            print("status: missed: " + video_id, file=statuslog)

        delete_ytmeta_raw(video)

    elif video.progress == 'invalid':
        if first:
            print("status: upload (possibly cached/bogus?): " + video_id, file=statuslog)
        else:
            print("status: upload: " + video_id, file=statuslog)

        delete_ytmeta_raw(video)

    elif video.progress == 'aborted':
        if first:
            print("status: aborted (possibly cached/bogus?): " + video_id, file=statuslog)
        else:
            print("status: aborted: " + video_id, file=statuslog)

        delete_ytmeta_raw(video)

    elif video.progress == 'downloading':
        if first:
            print("status: downloading (but this is wrong; we just started!): " + video_id, file=statuslog)

        wants_rescrape = False

        if pids.get(video_id):
            (pypid, dlpid) = pids[video_id]

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

        if wants_rescrape:
            # Check status
            downloader = ChatDownloader()
            youtube = downloader.create_session(YouTubeChatDownloader)

            details = None
            try:
                details = youtube.get_video_data(video_id)
            except Exception:
                pass

            if details and details.get('status') in {'live', 'upcoming'}:
                print("warning: downloader seems to have exited prematurely. reinvoking:", video_id, file=sys.stderr)
                invoke_downloader(video)
            else:
                video.set_progress('downloaded')

                try:
                    del pids[video_id]
                except KeyError:
                    pass

                persist_meta(video, fresh=True)
                delete_ytmeta_raw(video)

    elif video.progress == 'downloaded':
        if first:
            print("status: finished (cached?): " + video_id, file=statuslog)
        else:
            print("status: finished: " + video_id, file=statuslog)

            delete_ytmeta_raw(video)

    if not video.did_meta_flush:
        print("warning: didn't flush meta for video; flushing now", file=sys.stderr)
        persist_meta(video, fresh=True)

    video.did_progress_print = True
    statuslog.flush()


def handle_special_signal(signum, frame):
    os.makedirs('dump', exist_ok=True)

    with open("dump/lives", "w") as fp:
        for video in lives.values():
            # Fine as long as no objects in the class.
            fp.write(json.dumps(video.__dict__, sort_keys=True))

    with open("dump/pids", "w") as fp:
        fp.write(json.dumps(pids))

    with open("dump/general_stats", "w") as fp:
        fp.write(json.dumps(general_stats))

    with open("dump/staticconfig", "w") as fp:
        print("FORCE_RESCRAPE=" + str(FORCE_RESCRAPE), file=fp)
        print("DISABLE_PERSISTENCE=" + str(DISABLE_PERSISTENCE), file=fp)


rescrape = rescrape_ytdlp

invoke_scraper = invoke_scraper_ytdlp


# TODO
if __name__ == '__main__':
    mainpid = os.getpid()
    write_cgroup(mainpid)
    print(f"{mainpid = }")

    # Prep storage and persistent state directories
    os.makedirs('oo', exist_ok=True)
    os.chdir('oo')
    os.makedirs('by-video-id', exist_ok=True)
    os.makedirs('chat-logs', exist_ok=True)
    os.makedirs('pid', exist_ok=True)

    signal.signal(signal.SIGUSR1, handle_special_signal)

    print("Updating lives status", flush=True)
    update_lives_status()

    # Initial load
    print("Starting initial pass", flush=True)

    with open("discovery.txt", "a") as dlog:
        print("program started", file=dlog, flush=True)
        dlog.flush()
    statuslog = open("status.txt", "a")
    print("program started", file=statuslog)
    statuslog.flush()
    os.fsync(statuslog.fileno())

    if True:
        try:
            # Populate cache from disk
            for video_id, video in lives.items():
                progress = video.progress

                if progress == 'unscraped':
                    # Try to load missing meta from disk
                    recall_video(video_id)

            # Try to make sure downloaders are tracked with correct state
            process_dlpid_queue()

            # Scrape each video again if needed
            for video in lives.values():
                maybe_rescrape_initially(video)

            for video in lives.values():
                process_one_status(video, first=True)

        except KeyboardInterrupt:
            statuslog.flush()
            os.fsync(statuslog.fileno())
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during initial load processing") from exc

    statuslog.flush()

    print("Starting main loop", flush=True)
    while True:
        try:
            time.sleep(SCRAPER_SLEEP_INTERVAL)
            update_lives_status()

            # Try to make sure downloaders are tracked with correct state
            process_dlpid_queue()

            # Scrape each video again if needed
            for video in lives.values():
                maybe_rescrape(video)

            for video in lives.values():
                process_one_status(video)

        except KeyError:
            print("warning: internal inconsistency! squashing KeyError exception...", file=sys.stderr)

        except KeyboardInterrupt:
            statuslog.flush()
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during main loop processing") from exc

        finally:
            print("number of active children: " + str(len(mp.active_children())))   # side effect: joins finished tasks
            print("number of known lives: " + str(len(lives)))

            counters = {'progress': {}, 'status': {}, 'meta': 0, 'rawmeta': 0}
            for video in lives.values():
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
            print("number of tracked pid groups: " + str(len(pids)))
            print(end='', flush=True)

            statuslog.flush()
