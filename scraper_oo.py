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
    Set,
    Tuple
)

from bs4 import BeautifulSoup  # type: ignore
from urllib3.exceptions import SSLError  # type: ignore
from chat_downloader import ChatDownloader  # type: ignore
from chat_downloader.sites import YouTubeChatDownloader  # type: ignore
from chat_downloader.errors import (  # type: ignore
    UserNotFound,
    VideoNotFound,
    NoVideos,
    RetriesExceeded,
    ChatDownloaderError
)

from utils import (
    check_pid,
    get_timestamp_now,
    extract_video_id_from_yturl,
    json_stream_wrapper,
    meta_load_fast,
    meta_extract_start_timestamp,
    meta_extract_raw_live_status
)
from video import (
    BaseVideo,
    TransitionException,
    statuses,
    progress_statuses
)
from channel import (
    BaseChannel
)


try:
    from write_cgroup import write_cgroup
except ImportError:
    def write_cgroup(mainpid):
        pass


# Debug switch
DISABLE_PERSISTENCE = False
FORCE_RESCRAPE = False
ENABLE_ALTMAIN = False
ALLOW_COOKIED_COMMUNITY_TAB_SCRAPE = False
SCRAPER_SLEEP_INTERVAL = 60
CHANNEL_SCRAPE_LIMIT = 30
DUMP_DIR = 'dump'
DUMP_DIR_ALT = 'dump_alt'


downloadmetacmd = "../yt-dlp/yt-dlp.sh -s -q -j --ignore-no-formats-error "
downloadchatprgm = "../downloader.py"
channelscrapecmd = "../scrape_channel_oo.sh"
channelpostscrapecmd = "../scrape_community_tab.sh"
channelmemberscrapecmd = "../scrape_membership_tab.sh"
mainchannelsfile = "./channels.txt"
watchdogprog = "../watchdog.sh"
holoscrapecmd = 'wget -nv --load-cookies=../cookies-schedule-hololive-tv.txt https://schedule.hololive.tv/lives -O auto-lives_tz'
holoscrape_api_cmd = "wget -nv https://schedule.hololive.tv/api/list/1 -O - | jq '[.dateGroupList|.[]|.videoList|.[]|{datetime,isLive,platformType,url,title,name}|select(.platformType == 1)]' >| auto-lives_filt.json"


meta_lastresort_keys = {'_raw_player_response', '_raw_info_dict'}


# For alt-main usage
is_true_main = True


def _get_member_cookie_file(channel_id: str):
    if not is_true_main and channel_id is not None:
        return f"cookies/{channel_id}.txt"


class Video(BaseVideo):
    def rescrape_meta(self):
        """ Ignore known meta and fetch meta from YouTube. """
        cookie_file = _get_member_cookie_file(self.referrer_channel_id or self.meta and self.meta.get('channel_id'))
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


class Channel(BaseChannel):
    pass


def has_metafile_status(video: Video, status: str):
    if status not in statuses:
        raise ValueError('invalid status to has_metafile_status')

    return os.path.exists(f'by-video-id/{video.video_id}.meta.{status}') or os.path.exists(f'by-video-id/{video.video_id}.meta.{status}.simple')


def should_filter_video(video_id: str):
    """ Run recall_video on an dummy context to determine if we should exclude loading the video """
    tmp_context = AutoScraper()
    recall_video(video_id, context=tmp_context, filter_progress=True)
    tmp_video = tmp_context.lives.get(video_id)
    tmp_metafile_exists = getattr(tmp_video, 'metafile_exists', False)
    return tmp_video.meta is None and tmp_metafile_exists


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
        self.holoschedule_metachannel = Channel('holoschedule', is_metachannel=True)
        self.urllist_metachannel = Channel('urllist', is_metachannel=True)

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
                try:
                    last_batch = self.holoschedule_metachannel.batch
                except AttributeError:
                    # format migration
                    last_batch = self.holoschedule_metachannel = Channel('holoschedule', is_metachannel=True)

                ch = self.holoschedule_metachannel
                if ch.batching:
                    print("warning: batching in progress (holoschedule), resetting:", ch.channel_id, file=sys.stderr)
                    print("last batch:", ch.channel_id, file=sys.stderr)
                    print(ch.batch, file=sys.stderr)
                    ch.clear_batch()

                self.holoschedule_metachannel.start_batch()

                try:
                    self.update_lives_status_holoschedule(dlog=dlog)
                except Exception:
                    print("warning: exception during holoschedule scrape. Network error?")
                    traceback.print_exc()

                try:
                    self.update_lives_status_holoschedule_api(dlog=dlog)
                except Exception:
                    print("warning: exception during holoschedule api fetch. Network error?")
                    traceback.print_exc()

                self.holoschedule_metachannel.end_batch()

                # set difference
                vanished = (last_batch or set()) - (self.holoschedule_metachannel.batch or set())
                for video_id in vanished:
                    video = self.get_or_init_video(video_id, id_source='holoschedule:vanished')
                    if video.status in ['prelive', 'live']:
                        if not video.did_meta_flush:
                            persist_meta(video, context=self)
                        if has_metafile_status('postlive'):
                            print(f'holoschedule vanished video: {video.video_id} {video.status} -> postlive')
                            video.set_status('postlive')
                        elif video.status == 'prelive' and has_metafile_status('live'):
                            print(f'holoschedule vanished video: {video.video_id} {video.status} -> live')
                            video.set_status('live')
                        else:
                            video.rescrape_meta()

            except OSError:
                print("warning: OS (I/O) error during holoschedule follow-up processing.. Network error?")
                traceback.print_exc()

            except TransitionException:
                for channel in self.channels.values():
                    if channel.batching:
                        print("warning: batching in progress, resetting:", channel.channel_id, file=sys.stderr)
                        print("last batch:", channel.channel_id, file=sys.stderr)
                        print(channel.batch, file=sys.stderr)
                        channel.clear_batch()

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
        oldlives = 0
        currentlives = 0

        if dlog is None:
            dlog = sys.stdout

        for link in soup.find_all('a'):
            # Extract any link
            href = link.get('href')
            video_id = extract_video_id_from_yturl(href, strict=True)

            if video_id is None:
                continue

            if video_id not in self.lives:
                should_filter = should_filter_video(video_id)
                if should_filter:
                    # filter_progress excluded meta, which means we don't keep to keep this video around.
                    oldlives += 1
                    continue

                recall_video(video_id, context=self, filter_progress=True, id_source='holoschedule:html:disk', disk_only=True)

            video = self.get_or_init_video(video_id, id_source='holoschedule:html')
            if video.progress == 'unscraped':
                print("discovery: (htmlscrape) new live listed:", video_id, file=dlog, flush=True)
                if dlog != sys.stdout:
                    print("discovery: (htmlscrape) new live listed:", video_id, file=sys.stdout, flush=True)
                newlives += 1
            else:
                # known (not new) and current (not old) live listed
                currentlives += 1

        print("discovery: holoschedule: new lives:", str(newlives))
        print("discovery: holoschedule: current lives:", str(currentlives))
        print("discovery: holoschedule: old lives:", str(oldlives))

    def update_lives_status_holoschedule_api(self, /, *, dlog: IO = None) -> None:
        jsonlist = get_hololivetv_api_json()
        newlives = 0
        oldlives = 0
        currentlives = 0

        if dlog is None:
            dlog = sys.stdout

        rescrape_queue = []

        for video_info in jsonlist:
            # Extract any link
            url = video_info['url']
            is_live = video_info['isLive']
            video_id = extract_video_id_from_yturl(url, strict=True)

            if video_id is None:
                continue

            if video_id not in self.lives:
                should_filter = should_filter_video(video_id)
                if should_filter:
                    # filter_progress excluded meta, which means we don't keep to keep this video around.
                    oldlives += 1
                    continue

                recall_video(video_id, context=self, filter_progress=True, id_source='holoschedule:api:disk', disk_only=True)

            video = self.get_or_init_video(video_id, id_source='holoschedule:api')
            if video.progress == 'unscraped':
                print("discovery: (api) new live listed (live: {is_live}):", video_id, file=dlog, flush=True)
                if dlog != sys.stdout:
                    print(f"discovery: (api) new live listed (live: {is_live}):", video_id, file=sys.stdout, flush=True)
                newlives += 1
            else:
                # known (not new) and current (not old) live listed
                currentlives += 1
                if is_live and video.status == 'prelive':
                    rescrape_queue.append(video)

        for video in rescrape_queue:
            if video.status == 'prelive':
                # get the ytmeta as of the moment it went live.
                print('holoschedule (api) update task: prelive video is said to be live, rescraping to update status:', video.video_id)
                rescrape_chatdownloader(video)
                # important, or else next recall_video() will revert the status!
                persist_basic_state(video, context=self, clobber=True, clobber_pid=False)
                if not video.did_meta_flush:
                    persist_ytmeta(video, fresh=True, clobber=True)

        print("discovery: holoschedule (api): new lives:", str(newlives))
        print("discovery: holoschedule (api): current lives:", str(currentlives))
        print("discovery: holoschedule (api): old lives:", str(oldlives))

    def update_lives_status_urllist(self, *, dlog: IO = None):
        """ Process a url file (currently only supports raw video IDs)
            Can be called standalone.
        """
        try:
            ch = self.urllist_metachannel
        except AttributeError:
            # format migration
            ch = self.urllist_metachannel = Channel('urllist', is_metachannel=True)

        if ch.batching:
            print("warning: batching in progress (urllist), resetting:", ch.channel_id, file=sys.stderr)
            print("last batch:", ch.channel_id, file=sys.stderr)
            print(ch.batch, file=sys.stderr)
            ch.clear_batch()

        self.process_urllist_videos(channel=ch, dlog=dlog)

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
                    for line in channellist.readlines():
                        # ';' is comment leader
                        if not line or line.strip().startswith(';'):
                            continue

                        channel_info = line.split(';', 2)[0].split()
                        if len(channel_info) == 1:
                            channel_id, throttle = channel_info[0], 300.0
                        elif len(channel_info) == 2:
                            channel_id, throttle = channel_info[0], float(channel_info[1])
                        else:
                            print('warning: line from channels file has too many values', file=sys.stderr)
                            continue

                        self.scrape_and_process_channel(channel_id=channel_id, dlog=dlog, throttle=throttle)

        except Exception:
            print(f"warning: unexpected error with processing {channels_file}", file=sys.stderr)
            traceback.print_exc()
            for channel in self.channels.values():
                if channel.batching:
                    print("warning: batching in progress, resetting:", channel_id, file=sys.stderr)
                    print("last batch:", channel_id, file=sys.stderr)
                    print(channel.batch, file=sys.stderr)
                    channel.clear_batch()

    def _videolist_rescrape_uncached_meta(self, /, video: Video, *, origin: str, is_membership, channel: Channel) -> Tuple[bool, bool]:
        """ If no precached meta (from yt-dlp or another source), then rescrape.
            Return whether a lack of meta was discovered, and whether the scrape failed.
        """
        if video.meta is not None:
            return (False, False)

        metafile_exists = getattr(video, 'metafile_exists', False)
        cache_miss = False
        video_id = video.video_id

        vid_type_info = 'video'
        if is_membership:
            vid_type_info = 'member video'

        if origin == 'channellist':
            origin_text = 'channel'
            supp_info = f'for {vid_type_info} {video_id} on channel {channel.channel_id} ({metafile_exists = })'
        elif origin == 'urllist':
            origin_text = 'urllist'
            supp_info = f'for {vid_type_info} {video_id} via urllist ({metafile_exists = })'
        else:
            raise RuntimeError('invalid origin specified for videolist helper')

        if not is_membership:
            # We may be reloading old URLs after a program restart
            print('ytmeta cache miss', supp_info)
            if not metafile_exists:
                cache_miss = True
                rescrape(video)
                if video.meta is None:
                    # scrape failed
                    return (cache_miss, True)
                video.rawmeta = video.meta.get('raw')
                video.did_meta_flush = False
                video.meta_flush_reason = f'new meta (yt-dlp source, {origin_text} task origin, after cache miss)'
        else:
            print('ytmeta missing', supp_info)
            if not metafile_exists:
                rescrape(video, cookies=_get_member_cookie_file(channel.channel_id))
                if video.meta is None:
                    if not video.did_meta_flush:
                        print(f"warning: didn't flush meta for {origin_text} {vid_type_info}; flushing now", file=sys.stderr)
                        persist_ytmeta(video, fresh=True)
                    # scrape failed
                    return (cache_miss, True)
                video.rawmeta = video.meta.get('raw')
                video.did_meta_flush = False
                video.meta_flush_reason = f'new meta (yt-dlp source, {origin_text} task origin, after cache miss (cookied))'

        return (cache_miss, False)

    def process_urllist_videos(self, /, channel: Channel, *, dlog: IO = None, is_membership=False) -> None:
        """ Read user-specified video ID list, process each video ID, and persist the meta state. """
        if dlog is None:
            dlog = sys.stdout

        newlives = 0
        knownlives = 0
        numignores: Dict = {}
        channel.did_discovery_print = True

        allurl_file = "video_ids.txt"

        channel.start_batch()

        try:
            with open(allurl_file) as urls:
                for video_id in [f.split(" ")[0].strip() for f in urls.readlines()]:
                    # Process each recent video
                    should_filter = False
                    if video_id not in self.lives:
                        # For now, assume the worst and always load membership videos
                        should_filter = should_filter_video(video_id)
                        if is_membership:
                            word = 'member'
                        else:
                            word = 'main'
                        if should_filter and is_membership:
                            should_filter = False
                            print(f"notice: existing member live: {video_id} on urllist (forcibly not filtered!)", flush=True)
                        if not should_filter:
                            recall_video(video_id, context=self, filter_progress=True, id_source=f'urllist:urllist:{word}:disk', disk_only=True)
                        else:
                            continue

                    video = self.lives.get(video_id)
                    if not video:
                        print(f'warning: video not loaded, skipping: {video_id}', file=sys.stderr)
                        # this will screw up our counters but it's better than skipping the loop
                        continue

                    channel.add_video(video)

                    if not channel.did_discovery_print:
                        metastatus_ok = video.meta is not None
                        if not metastatus_ok:
                            if is_membership:
                                word = 'member'
                            else:
                                word = 'main'
                            recall_video(video_id, context=self, filter_progress=True, id_source=f'urllist:urllist:{word}')
                        metastatus_ok = video.meta is not None
                        metafile_exists = getattr(video, 'metafile_exists', False)
                        if metafile_exists:
                            # avoid 'extra' rescrapes based on status
                            video.did_status_print = True
                        print(f"discovery: new live listed: {video_id} on urllist (meta loaded: {metastatus_ok}; metafile exists: {metafile_exists})", file=dlog, flush=True)
                        # TODO: accumulate multiple videos at once.
                        channel.did_discovery_print = True
                        newlives += 1
                    else:
                        # known (not new) live listed (channel unaware)
                        knownlives += 1

                    saved_progress = video.progress

                    if not FORCE_RESCRAPE and saved_progress in {'downloaded', 'missed', 'invalid', 'aborted'}:
                        numignores[saved_progress] = numignores.setdefault(saved_progress, 0) + 1

                        # meta likely won't be loaded for this to have too much effect, if filter_progress was set when recalling.
                        delete_ytmeta_raw(video, context=self, suffix=" (urllist)")

                        continue

                    cache_miss, scrape_failed = self._videolist_rescrape_uncached_meta(video, origin='urllist', is_membership=is_membership, channel=channel)
                    if scrape_failed:
                        continue

                    # There's an optimization opportunity for reducing disk flushes here, but we forego it

                    # has parity with maybe_rescrape(); should only need to be called on new videos
                    if video.progress == 'unscraped':
                        process_ytmeta(video)

                    # has parity with maybe_rescrape() as well
                    if cache_miss or (saved_progress not in {'missed', 'invalid'} and saved_progress != video.progress):
                        persist_meta(video, context=self, fresh=True)

                    if not video.did_meta_flush:
                        print("warning: didn't flush meta for urllist video; flushing now", file=sys.stderr)
                        persist_ytmeta(video, fresh=True)

        except IOError:
            print("warning: unexpected I/O error when processing urllist scrape results", file=sys.stderr)
            traceback.print_exc()

        channel.end_batch()

        if len(channel.batch) > 0:
            print("discovery: video id list: new lives on urllist: " + str(newlives))
            print("discovery: video id list: known lives on urllist: " + str(knownlives))
            for progress, count in numignores.items():
                print("discovery: video id list: skipped ytmeta fetches on urllist: " + str(count) + " skipped due to progress state '" + progress + "'")

        channel.clear_batch()

    # TODO: rewrite
    def process_channel_videos_ytdlp(self, /, channel: Channel, *, dlog: IO = None, is_membership=False) -> None:
        """ Read scraped channel video list, process each video ID, and persist the meta state. """
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
                    should_filter = False
                    if video_id not in self.lives:
                        # For now, assume the worst and always load membership videos
                        should_filter = should_filter_video(video_id)
                        if is_membership:
                            word = 'member'
                        else:
                            word = 'main'
                        if should_filter and is_membership:
                            should_filter = False
                            print(f"notice: existing member live: {video_id} on channel {channel_id} (forcibly not filtered!)", flush=True)
                        if not should_filter:
                            recall_video(video_id, context=self, filter_progress=True, id_source=f'channel:urllist:{word}:disk', referrer_channel_id=channel_id, disk_only=True)
                        else:
                            continue

                    video = self.lives.get(video_id)
                    if not video:
                        print(f'warning: video not loaded, skipping: {video_id}', file=sys.stderr)
                        # this will screw up our counters but it's better than skipping the loop
                        continue

                    channel.add_video(video)

                    if not channel.did_discovery_print:
                        metastatus_ok = video.meta is not None
                        if not metastatus_ok:
                            if is_membership:
                                word = 'member'
                            else:
                                word = 'main'
                            recall_video(video_id, context=self, filter_progress=True, id_source=f'channel:urllist:{word}', referrer_channel_id=channel_id)
                        metastatus_ok = video.meta is not None
                        metafile_exists = getattr(video, 'metafile_exists', False)
                        if metafile_exists:
                            # avoid 'extra' rescrapes based on status
                            video.did_status_print = True
                        print(f"discovery: new live listed: {video_id} on channel {channel_id} (meta loaded: {metastatus_ok}; metafile exists: {metafile_exists})", file=dlog, flush=True)
                        # TODO: accumulate multiple videos at once.
                        channel.did_discovery_print = True
                        newlives += 1
                    else:
                        # known (not new) live listed (channel unaware)
                        knownlives += 1

                    saved_progress = video.progress

                    if not FORCE_RESCRAPE and saved_progress in {'downloaded', 'missed', 'invalid', 'aborted'}:
                        numignores[saved_progress] = numignores.setdefault(saved_progress, 0) + 1

                        # meta likely won't be loaded for this to have too much effect, if filter_progress was set when recalling.
                        delete_ytmeta_raw(video, context=self, suffix=" (channel)")

                        continue

                    cache_miss, scrape_failed = self._videolist_rescrape_uncached_meta(video, origin='channellist', is_membership=is_membership, channel=channel)
                    if scrape_failed:
                        continue

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

    def scrape_and_process_channel(self, channel_id, *, dlog: IO = None, throttle=300.0) -> None:
        """ Scrape a channel, with fallbacks.
            Can be called standalone.
        """
        channel = None
        use_ytdlp = False
        throttle = float(throttle)

        if dlog is None:
            dlog = sys.stdout

        if channel_id in self.channels:
            channel = self.channels[channel_id]

            elapsed = get_timestamp_now() - channel.batch_end_timestamp
            if elapsed < 0:
                print(f'warning: channel batch-end timestamp is in the future: ahead by {(-elapsed)} (channel: {channel_id})', file=sys.stderr)
            if elapsed < throttle:
                print(f'notice: skipping channel scrape (channel: {channel_id}) as time elapsed is too short ({elapsed:0.3F} < {throttle})', file=sys.stderr)
                return

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
                if channel.batching:
                    print("warning: batching in progress, resetting:", channel_id, file=sys.stderr)
                    print("last batch:", channel_id, file=sys.stderr)
                    print(channel.batch, file=sys.stderr)
                    channel.clear_batch()
                use_ytdlp = True

        if use_ytdlp:
            is_membership = not is_true_main
            self.scrape_and_process_channel_ytdlp(channel, dlog=dlog, is_membership=is_membership, throttle=throttle)

        # Scrape community tab page for links (esp. member stream links)
        # Currently only try this when cookies are provided.
        # TODO: use membership page instead, since it only includes (recent?) member videos and posts.
        # NOTE: alt_main is a preferable solution and tries the above tab page.
        # I believe this is a new feature by YouTube (~Nov 2021).
        if ALLOW_COOKIED_COMMUNITY_TAB_SCRAPE:
            if os.path.exists(channel_id + ".txt"):
                self.scrape_and_process_channel_ytdlp(channel, dlog=dlog, community_scrape=True, throttle=throttle)

    def scrape_and_process_channel_chatdownloader(self, /, channel: Channel, *, dlog: IO = None):
        """ Use chat_downloader's get_user_videos() to quickly get channel videos and live statuses. """
        if dlog is None:
            dlog = sys.stdout

        downloader = ChatDownloader()

        # Forcefully create a YouTube session
        youtube: YouTubeChatDownloader = downloader.create_session(YouTubeChatDownloader)

        limit = CHANNEL_SCRAPE_LIMIT
        PAGE_DELAY = 0.004
        ATTEMPT_DELAY = 0.06
        count = 0
        perpage_count = 0
        valid_count = 0
        skipped = 0

        seen_vids: Set[str] = set()
        lives = self.lives

        channel.start_batch()

        status_hints = []

        # Subcategorization on "Videos" tab
        video_categories = ['upcoming', 'live', 'all', 'past']

        counters_seen: Dict[str, Any] = dict(map(lambda e: (e, 0), video_categories))
        counters: Dict[str, Any] = dict(map(lambda e: (e, 0), video_categories))

        # We don't just check 'all' since the list used may be slow to update.
        for video_status in video_categories:
            attempts_left = 3
            while attempts_left > 0:
                try:
                    perpage_count = 0
                    time.sleep(ATTEMPT_DELAY)
                    for basic_video_details in youtube.get_user_videos(channel_id=channel.channel_id, video_status=video_status, params={'max_attempts': 3}):
                        time.sleep(PAGE_DELAY)
                        attempts_left -= 1
                        status = 'unknown'
                        status_hint: Optional[str] = None
                        just_scraped = False

                        video_id = basic_video_details.get('video_id')

                        try:
                            # Quickly discern whether a video is upcoming, live or past by the view text.
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
                            # view_count wasn't a valid key, so try to handle it
                            if video_id is not None and video_id in lives and lives[video_id].progress not in {'unscraped', 'aborted'} and lives[video_id].status not in {'postlive', 'upload'}:
                                print(f"warning: status hint extraction: no view count visible (likely 0). {seen_vids = } ... {basic_video_details = })", file=sys.stderr)
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

                        status_hints.append((video_id, status))

                        perpage_count += 1
                        if perpage_count >= limit:
                            if video_id in seen_vids or status == 'unknown' or (video_id in lives and lives[video_id].progress != 'unscraped'):
                                # would advance to next list, don't forget to count.
                                # perpage limit reached
                                if video_id not in seen_vids:
                                    count += 1
                                    seen_vids.add(video_id)
                                if status != 'unknown' and not (video_id in lives and lives[video_id].progress != 'unscraped'):
                                    counters_seen[video_status] += 1
                                    skipped += 1
                                break

                        if video_id in seen_vids:
                            # discovered on a prior list
                            continue
                        else:
                            # a different video was listed
                            count += 1
                            seen_vids.add(video_id)

                        if status == 'unknown':
                            # ignore past streams/uploads
                            continue

                        # video isn't an archive (is prelive/live)
                        counters_seen[video_status] += 1

                        if video_id in lives and lives[video_id].progress != 'unscraped':
                            # already known (esp. if listed in 'live' list)
                            skipped += 1
                            continue

                        # video isn't an archive and is unique so far in our channel search
                        valid_count += 1

                        if status != 'unknown':
                            print(f"discovery: new live listed (chat_downloader channel extraction, status: {status}, list: {video_status}): " + video_id, file=sys.stdout, flush=True)
                            if dlog != sys.stdout:
                                print(f"discovery: new live listed (chat_downloader channel extraction, status: {status}): " + video_id, file=dlog, flush=True)

                        video = self.get_or_init_video(video_id, id_source=f'channel:{video_status}', referrer_channel_id=channel.channel_id)
                        counters[video_status] += 1

                        channel.add_video(video)

                        vid_attempts_left = 3
                        while vid_attempts_left > 0:
                            vid_attempts_left -= 1
                            try:
                                # rescrape and process a new video
                                if not just_scraped:
                                    rescrape_chatdownloader(video, channel=channel, youtube=youtube)
                                    just_scraped = True

                                if (video.status != status or not video.did_status_print) and video.status not in ['unknown', 'aborted', 'postlive']:
                                    # Note; this only covers manually added channels in channels.txt; holoschedule isn't covered (yet).
                                    # Using the holoschedule JSON API will help with determinining if a video is live without having
                                    # to add more channels to the channel scrape task (which hits YouTube a bit harder).
                                    if not video.did_status_print:
                                        print(f'status appears to have changed ({video.status_flush_reason}); maybe rescraping:', video.video_id)
                                        video.did_status_print = True
                                    else:
                                        print(f'status appears to have changed ({video.status} -> {status}); maybe rescraping:', video.video_id)
                                    if not just_scraped:
                                        rescrape_chatdownloader(video, channel=channel, youtube=youtube)

                                persist_meta(video, context=self, fresh=True, clobber_pid=False)

                            except VideoNotFound:
                                print(f'warning: "Video not found" when scraping videos tab via chat_downloader; video retries left: {vid_attempts_left}', file=sys.stderr)
                                time.sleep(0.1)

                            else:
                                vid_attempts_left = 0

                        if perpage_count >= limit:
                            # perpage limit reached
                            break

                    if count >= limit * len(video_categories):
                        print(f"limit of {limit} reached")
                        break

                except UserNotFound:
                    print(f'warning: "User not found" when scraping videos tab via chat_downloader; retries left: {attempts_left}', file=sys.stderr)

                except SSLError:
                    # a bug in chat_downloader means some optionally-handled exceptions in "requests" are not wrapped in
                    # requests.exceptions.RequestException but is instead raised by urllib. This mean certain exceptions
                    # can be unhandled; SSLError is the most common one.
                    print(f'warning: SSL error when scraping videos tab via chat_downloader; retries left: {attempts_left}', file=sys.stderr)

                except NoVideos:
                    print('warning: "No videos" when scraping videos tab via chat_downloader; not retrying', file=sys.stderr)
                    attempts_left = 0

                else:
                    break

            else:
                if attempts_left == 0:  # out of attempts
                    raise RuntimeError('too many retries on channel scrape with chat_downloader')

        channel.end_batch()

        for video_id, status_hint in status_hints:
            if video_id not in self.lives:
                # Don't create new video objects needlessly, else they will be rescraped.
                continue
            video = self.get_or_init_video(video_id, id_source='channel:across', referrer_channel_id=channel.channel_id)
            if video.status == 'prelive' and status_hint == 'live' \
                    or video.status in ['prelive', 'live'] and status_hint == 'postlive':
                # Existing videos will not see the suggestion to rescrape since the tab loop skips the processing for them.
                print(f'channel scraper: tab page suggests new live status, rescraping: {video_id}: "{video.status}" became "{status_hint}"')
                video.rescrape_meta()
                persist_meta(video, context=self, fresh=True, clobber=True, clobber_pid=False)
            elif video.status == 'prelive' and video.progress in ['missed', 'aborted', 'downloaded']:
                print(f'warning: status hint contradicts very unlikely video progress: video {video_id} with prior status {video.status}: hint is {status_hint}, progress is {video.progress}', file=sys.stderr)
                video.rescrape_meta()
                persist_meta(video, context=self, fresh=True, clobber=True)
            elif video.status == 'live' and video.progress in ['missed', 'aborted', 'downloaded']:
                print(f'warning: status hint contradicts unlikely video progress: video {video_id} with prior status {video.status}: hint is {status_hint}, progress is {video.progress}', file=sys.stderr)
                # be mindful that this may catch the tail-end of lives
                now = get_timestamp_now()
                trans_ts = video.transition_timestamp
                if now - trans_ts > 60.0:
                    video.rescrape_meta()
                    persist_meta(video, context=self, fresh=True, clobber=True)
                else:
                    print(f'warning: status hint contradiction ignored; likely the live just ended: video {video_id}', file=sys.stderr)

        report_text = ""
        for vs in ['upcoming', 'live', 'all']:
            report_text += f"{vs} {counters[vs]}/{counters_seen[vs]}; "

        print(f"discovery: channels list (via chat_downloader): channel {channel.channel_id} new lives/total: " + report_text + f"{(valid_count)}/{skipped} across; {count} processed)")

    def scrape_and_process_channel_ytdlp(self, /, channel: Channel, *, dlog: IO = None, community_scrape=False, is_membership=False, throttle=300.0):
        """ Scrape and process a channel using yt-dlp, but throttle """
        channel_id = channel.channel_id
        if channel_id in self.channels:
            elapsed = get_timestamp_now() - channel.batch_end_timestamp
            if elapsed < 0:
                print(f'warning: channel batch-end timestamp is in the future: ahead by {(-elapsed)} (channel: {channel_id})', file=sys.stderr)
            if elapsed < throttle:
                print(f'notice: skipping yt-dlp channel scrape (channel: {channel_id}) as time elapsed is too short ({elapsed:0.3F} < {throttle})', file=sys.stderr)
                return

        self.invoke_channel_scraper_ytdlp(channel, membership_scrape=is_membership)
        self.process_channel_videos_ytdlp(channel, dlog=dlog, is_membership=is_membership)

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
                rawjson = None
                try:
                    rawjson = json.loads(jsonres)
                    metalist.append(populate_meta_fields_ytdlp(rawjson))
                except KeyError:
                    if rawjson:
                        ytmeta = {'_raw_info_dict': rawjson}
                        ytmeta['_scrape_provider'] = 'yt-dlp'
                        metalist.append(ytmeta)
                    print(f"warning: channel scrape: missing field in yt-dlp raw info-dict (video_id: {rawjson.get('id')})", file=sys.stderr)
                    traceback.print_exc()
                except Exception:
                    if community_scrape:
                        print("warning: exception in channel post scrape task (corrupt meta?)", file=sys.stderr)
                    else:
                        print("warning: exception in channel scrape task (corrupt meta?)", file=sys.stderr)
                    traceback.print_exc()

            for ytmeta in metalist:
                video_id = ytmeta["id"]
                # Before loading the video into our video list, check if we want to filter it out.
                should_filter = should_filter_video(video_id)
                if should_filter:
                    # filter_progress excluded meta, which means we don't keep to keep this video around.
                    print(f"ignoring video from channel scrape; meta no longer required and exists on disk (video = {video_id})")
                    continue

                # Likely a new or unfinished video
                recall_video(video_id, context=self, filter_progress=True, id_source='channel:ytdlp_metalist', referrer_channel_id=channel.channel_id)
                video = self.lives.get(video_id)
                metafile_exists = getattr(video, 'metafile_exists', False)
                if video and video.meta is None and not metafile_exists:
                    print(f'{metafile_exists = }')
                    video.meta = ytmeta
                    video.rawmeta = ytmeta.get('raw')
                    video.did_meta_flush = False
                    video.meta_flush_reason = 'new meta (yt-dlp source, channel task origin)'
                else:
                    if community_scrape and not membership_scrape:
                        print(f"ignoring ytmeta from channel community tab scrape (video = {video_id}, {metafile_exists = })")
                    elif membership_scrape:
                        print(f"ignoring ytmeta from channel membership tab scrape (video = {video_id}, {metafile_exists = })")
                    else:
                        print(f"ignoring ytmeta from channel scrape (video = {video_id}, {metafile_exists = })")


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


def get_hololivetv_api_json():
    """ Get the latest JSON API result tied to the older site's schedule """
    subprocess.run(holoscrape_api_cmd, shell=True)

    return json.load(open('auto-lives_filt.json'))


def rescrape_chatdownloader(video: Video, *, channel=None, youtube=None, cookies=None, throttle=15.0) -> None:
    """ rescrape_ytdlp, but using chat_downloader
        Interpret yt-dlp rawmeta.
        Populates meta fields.
    """
    video_id = video.video_id
    if video.meta_timestamp:
        elapsed = get_timestamp_now() - video.meta_timestamp
        if elapsed < 0:
            print(f'warning: elapsed time since scrape is negative: {elapsed} (video: {video_id})', file=sys.stderr)
        if elapsed < throttle:
            print(f'warning: throttling scrape for video {video_id} ({elapsed:0.6F} < {throttle})', file=sys.stderr)
            return

    video_data, player_response, status = invoke_scraper_chatdownloader(video_id, youtube=youtube, skip_status=False, cookies=cookies)

    # keep only known useful fields, junk spam/useless fields
    old_player_response = player_response
    player_response = {}
    dubious_status = False
    for key in ['playabilityStatus', 'videoDetails', 'microformat']:
        val = old_player_response.get(key)
        if val is None:
            print(f'warning: expected key \'{key}\' is missing for video {video.video_id}', file=sys.stderr)
            dubious_status = True
        else:
            player_response[key] = val
    for key in ['streamingData']:
        player_response[key] = old_player_response.get(key)
    del old_player_response

    try:
        meta = populate_meta_fields_chatdownloader(player_response=player_response, video_data=video_data, channel=channel, video_id=video_id)
    except KeyError:
        if not dubious_status:
            print(f'warning: meta field extraction threw an unexpected error; video {video_id} has remote status {status}', file=sys.stderr)
            traceback.print_exc()
        # potentially a memory leak
        meta = {"_raw_player_response": player_response}
        meta['_scrape_provider'] = 'chat_downloader'

    if not dubious_status:
        if video.status == 'postlive' and status in ['prelive', 'live']:
            # this can indicate a failed scrape and it being marked "downloaded" prematurely; attempt to remedy this.
            if video.progress in {'downloaded', 'missed', 'aborted'}:
                print(f'warning: video {video.video_id} is marked as done but new status is {status}! (progress = {video.progress}) Attempting to correct...', file=sys.stderr)
                video.set_status(status)
                video.reset_progress()
                process_ytmeta(video)
            else:
                print(f'warning: video {video.video_id} has wrong status (was {video.status}, now {status})! (progress = {video.progress})')
                video.set_status(status)
        else:
            video.set_status(status)
    else:
        print(f'notice: video {video_id}: final status = {video.status}; final progress = {video.progress}; new status has reported error', file=sys.stderr)
        video.set_status('error')

    video.did_meta_flush = False
    word = None
    if video.meta is None:
        word = 'new'
    else:
        word = 'updated'
        if video.meta == meta:
            # this would be unlikely, since there should be timestamps involved.
            word = 'redundant'
    if meta.get('raw') is None:
        word += " incomplete"
    elif dubious_status:
        word += " suspicious"
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
        May throw KeyError on private videos.
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
    try:
        meta['description'] = microformat['description']['simpleText']
    except KeyError:
        # Video lacks a description
        meta['description'] = None
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

    print(f'[scraper chatdownloader]: {video_id = } {cookies = }')

    e = None
    for _ in range(3):
        try:
            video_data, player_response, *_ = youtube._parse_video_data(video_id, params={'max_attempts': 2})
        except SSLError as se:
            e = se
            # a bug in chat_downloader; just retry
            time.sleep(0.1)
        else:
            break
    try:
        _ = video_data
    except (NameError, UnboundLocalError):
        raise RuntimeError from e

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
        action = 'Updating'
        if not clobber or not os.path.exists(statefile):
            action = 'Creating'
        print(f'{action} statefile {statefile}')
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


def convert_scraper_status_to_ytdlp_status(scraper_status: str):
    raise NotImplementedError


def convert_ytdlp_status_to_scraper_status(live_status: str):
    """ Get equivalent scraper-internal string for known info-dict live_status values.
        Default is 'unknown'.
    """
    if live_status == 'is_live':
        return 'live'
    if live_status == 'is_upcoming':
        return 'prelive'
    if live_status == 'was_live':
        return 'postlive'
    if live_status == 'not_live':
        return 'upload'

    return 'unknown'


def check_ytmeta_status_correspondence(*, status, meta):
    """ Compare live_status meta field to video.status and check if they are equivalent """
    if not meta.get('live_status'):
        # meta may be corrupt or invalid
        return None

    if meta.get('title') is None or meta.get('uploader') is None:
        # probably a failed scrape
        return None

    live_status = meta['live_status']
    scraper_status = convert_ytdlp_status_to_scraper_status(live_status)
    return status == scraper_status


def get_meta_supp_status(video: Video):
    """ supplementary status; can be '', 'null', 'incomplete', or 'simple' """
    if video.rawmeta is None:
        if not video.meta:
            return 'null'
        if video.meta.get('title') is None or video.meta.get('uploader') is None:
            # probably a failed scrape
            return 'incomplete'
        else:
            return 'simple'

    return ''


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
            supp_status = get_meta_supp_status(video)
            if supp_status:
                metafileyt_status += "." + supp_status

            if supp_status == 'simple':
                try:
                    status = video.status
                    status_from_ytmeta = ytmeta['ytmeta']['live_status']
                    if check_ytmeta_status_correspondence(status=status, meta=ytmeta['ytmeta']) is False:
                        print(f'warning: ytmeta does not match expected status: "{status_from_ytmeta}" and "{status}" do not correspond.')
                except KeyError:
                    print('warning: cannot verify that meta matches the live status')

        try:
            if clobber or not os.path.exists(metafileyt):
                action = 'Updating'
                if not clobber or not os.path.exists(metafileyt):
                    action = 'Creating'
                print(f'{action} {metafileyt}')
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

                    action = 'Updating'
                    if not clobber or not os.path.exists(metafileyt_status):
                        action = 'Creating'
                    print(f'{action} {metafileyt_status}')
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
def recall_video(video_id: str, *, context: AutoScraper, filter_progress=False, id_source=None, referrer_channel_id=None, disk_only=False):
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
                print(f'warning: program meta failed to load even though the file exists: {video_id}', file=sys.stderr)
                valid_meta = False

        # Reduce memory usage by not loading ytmeta for undownloadable videos
        if valid_meta and filter_progress:
            should_ignore = meta['status'] in {'postlive', 'upload'} and meta['progress'] != 'unknown'
            should_ignore = should_ignore or meta['progress'] in {'downloaded', 'missed', 'invalid'}
            should_ignore = should_ignore or meta['status'] == 'error' and meta['progress'] == 'aborted'

        # note: FORCE_RESCRAPE might clobber old ytmeta if not loaded (bad if the video drastically changes or goes unavailable)
        if valid_ytmeta and not should_ignore:
            with open(metafileyt, 'rb') as fp:
                try:
                    ytmeta = json.loads(fp.read())
                    valid_ytmeta = 'ytmeta' in ytmeta

                except (json.decoder.JSONDecodeError, KeyError):
                    valid_ytmeta = False

    else:
        if disk_only:
            # Avoid video instantiation
            return

    # This has to be conditional, unless we want old references to be silently not updated and have tons of debugging follow.
    video = context.get_or_init_video(video_id, id_source=id_source, referrer_channel_id=referrer_channel_id)

    if valid_meta:
        # Commit status to runtime tracking (else we would discard it here)
        # Direct assignment here to avoid checks, might rewrite later
        video.status = meta['status']
        video.progress = meta['progress']

        if valid_ytmeta:
            if not should_ignore:
                video.meta = typing.cast(Dict[str, Any], ytmeta['ytmeta'])
                video.rawmeta = video.meta.get('raw')
                if video.rawmeta is not None:
                    del video.meta['raw']
            else:
                # Allows code that checks for meta to not rescrape if it is missing
                # Note that this means the metafile isn't validated, which could be good or bad.
                video.metafile_exists = True

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
    """ Set status, initial progress from meta
        It should be save to call this function multiple times, as long as we don't care to lose the current set progress.
    """
    if video.meta is None:
        raise RuntimeError('precondition failed: called process_ytmeta but ytmeta for video ' + video.video_id + ' not found.')

    if video.meta.get('is_upcoming'):
        # note: premieres can also be upcoming but are not livestreams.
        video.set_status('prelive')
        if video.progress == 'unscraped':
            video.set_progress('waiting')

    elif video.meta.get('is_live'):
        video.set_status('live')
        if video.progress == 'unscraped':
            video.set_progress('waiting')

    elif video.meta.get('is_livestream') or video.meta.get('live_endtime'):
        # note: premieres also have a starttime and endtime
        video.set_status('postlive')
        if video.progress == 'unscraped':
            video.set_progress('missed')

    elif video.meta and meta_lastresort_keys.intersection(video.meta):
        present_lastresort_keys = [x for x in meta_lastresort_keys.intersection(video.meta)]
        print(f'warning: process_ytmeta failed ({present_lastresort_keys[0]} detected, field export failed?)', file=sys.stderr)
        return None

    else:
        video.set_status('upload')
        video.set_progress('invalid')


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


def maybe_rescrape_initially(video: Video, *, context: AutoScraper):
    if video.progress in {'waiting', 'downloading'}:
        # Recover from crash or interruption
        print(f"(initial check) video {video.video_id}: resetting progress after possible crash: {video.progress} -> unscraped")
        video.reset_progress()

    if video.progress == 'missed' and video.status in {'unknown', 'prelive'}:
        # Recover from potential corruption or bug
        print(f"(initial check) video {video.video_id}: resetting progress after possible bug: {video.progress} -> unscraped. found status: {video.status}")
        video.reset_progress()

    if video.progress == 'aborted' and video.status in {'unknown', 'prelive', 'live'}:
        # Recover from server-induced error
        print(f"(initial check) video {video.video_id}: resetting progress after possible server-induced abort: {video.progress} -> unscraped. found status: {video.status}")
        video.reset_progress()

    if video.progress == 'unscraped' or FORCE_RESCRAPE:
        video.rescrape_meta()
        if video.meta is None:
            # initial scrape failed
            return

        process_ytmeta(video)

    # Redundant, but purges corruption
    persist_meta(video, context=context, fresh=True)


def populate_meta_fields_ytdlp(jsonres) -> Dict[str, Any]:
    """ Interpret yt-dlp rawmeta.
        Populates meta fields.
    """
    ytmeta: Dict[str, Any] = {}
    ytmeta['_scrape_provider'] = 'yt-dlp'
    ytmeta['raw'] = jsonres
    ytmeta['id'] = jsonres['id']
    ytmeta['title'] = jsonres['title']
    ytmeta['description'] = jsonres.get('description')
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
    if not video.did_meta_flush and video.meta is not None:
        print(f'warning: attempting to clear rawmeta for video {video.video_id} without flushing, meta may be lost.', file=sys.stderr)

    # unprocessable rawmeta gets saved here as a last resort; we don't want it loaded since it won't be used.
    if video.meta and meta_lastresort_keys.intersection(video.meta):
        keyname = 'lastresort ytmeta del successes'
        if suffix:
            keyname = keyname + suffix
        general_stats[keyname] = general_stats.setdefault(keyname, 0) + 1
        for lastresort_key in meta_lastresort_keys:
            if lastresort_key in video.meta:
                del video.meta[lastresort_key]

    # FIXME: I don't see how a KeyError could raise?
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
    # Try to capture ytmeta changes
    if not video.did_status_print:
        print(f'remote status changed for video {video.video_id}:', video.status_flush_reason)
        video.did_status_print = True

        # Rescrape meta for saving, even though we don't really need it.
        if video.status in ['live', 'postlive']:
            if not os.path.exists(f'by-video-id/{video.video_id}.meta.{video.status}'):
                rescrape_chatdownloader(video)

    # Process only on change
    if video.did_progress_print:
        if not force:
            return
        else:
            print(f'forced progress update for video {video.video_id}')
    else:
        print(f'progress update for video {video.video_id} (source: {video.id_source or "unknown"}; channel: {video.referrer_channel_id or "unknown"}; reason: {video.progress_flush_reason})')
        video.did_progress_print = True

    video_id = video.video_id
    started_download = False

    statuslog = _get_status_log()
    if statuslog is None:
        statuslog = sys.stdout

    if video.progress == 'waiting':
        if video.meta is None:
            print("error: video.meta missing for video " + video_id, file=sys.stderr)
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
                # once more, for postlive ytmeta
                try:
                    rescrape_chatdownloader(video, youtube=youtube)
                except RetriesExceeded:
                    print('warning: postlive rescrape failed while downloading meta, connectivity issue?', file=sys.stderr)
                    traceback.print_exc()
                persist_meta(video, context=context, fresh=True, clobber=True)
                delete_ytmeta_raw(video, context=context)

                if video.status == 'error' and video.progress in ['unscraped', 'waiting', 'downloading']:
                    if video.progress not in ['downloaded', 'aborted']:
                        try:
                            video.set_progress('downloaded')
                        except TransitionException:
                            video.set_progress('aborted')

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


def dump_lives(context: AutoScraper, *, dest_dir=DUMP_DIR):
    with open(f"{dest_dir}/lives", "w") as fp:
        for video in context.lives.values():
            # Fine as long as no objects in the class.
            fp.write(json.dumps(video.__dict__, sort_keys=True))


def dump_pids(context: AutoScraper, *, dest_dir=DUMP_DIR):
    with open(f"{dest_dir}/pids", "w") as fp:
        fp.write(json.dumps(context.pids))


def dump_misc(context: AutoScraper, *, dest_dir=DUMP_DIR):
    with open(f"{dest_dir}/general_stats", "w") as fp:
        fp.write(json.dumps(context.general_stats))

    with open(f"{dest_dir}/staticconfig", "w") as fp:
        print("FORCE_RESCRAPE=" + str(FORCE_RESCRAPE), file=fp)
        print("DISABLE_PERSISTENCE=" + str(DISABLE_PERSISTENCE), file=fp)
        print("SCRAPER_SLEEP_INTERVAL=" + str(SCRAPER_SLEEP_INTERVAL), file=fp)
        print("CHANNEL_SCRAPE_LIMIT=" + str(CHANNEL_SCRAPE_LIMIT), file=fp)


def handle_debug_signal(signum, frame):
    if os.getpid() != mainpid:
        if is_true_main:
            print('warning: got debug signal, but mainpid doesn\'t match', file=sys.stderr)
            return

    dest_dir = DUMP_DIR
    if not is_true_main:
        dest_dir = DUMP_DIR_ALT

    os.makedirs(dest_dir, exist_ok=True)

    try:
        dump_lives(context=main_autoscraper, dest_dir=dest_dir)
    except Exception:
        print('debug: dumping lives failed.')
        traceback.print_exc()
    else:
        print('debug: dumping lives succeeded.')

    dump_pids(context=main_autoscraper, dest_dir=dest_dir)
    dump_misc(context=main_autoscraper, dest_dir=dest_dir)


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

    dest_dir = DUMP_DIR
    if not is_true_main:
        dest_dir = DUMP_DIR_ALT

    if not os.path.exists(dest_dir):
        os.chdir('oo')
    if not os.path.exists(dest_dir):
        print(f'reexec: cannot load from dump; dump directory "{dest_dir}" not found')
        return False

    try:
        # FIXME: something that doesn't rely on the shell not splitting the path, should we ever allow dumpdir to change.
        os.system(f'jq -as <{dest_dir}/lives >{dest_dir}/lives.jq')
        with open(f"{dest_dir}/lives.jq", "r") as fp:
            jsonres = json.load(fp)
            for viddict in jsonres:
                video = Video('XXXXXXXXXXX')
                video.__dict__ = viddict

                # Before loading the video into our video list, check if we want to filter it out.
                should_filter = should_filter_video(video.video_id)
                if should_filter and video.did_meta_flush:
                    # filter_progress excluded meta, which means we don't keep to keep this video around.
                    print(f"(reexec) ignoring video from video dump; meta no longer required and exists on disk (video = {video.video_id})")
                    continue

                main_autoscraper.lives[video.video_id] = video
    except Exception:
        print('reexec: recalling lives failed.')
        main_autoscraper.lives = {}
        traceback.print_exc()
        return False
    else:
        print('reexec: recalling lives succeeded.')

    try:
        with open(f"{dest_dir}/pids", "r") as fp:
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

    print_autoscraper_statistics(context=main_autoscraper)

    return True


def restart():
    global mainpid
    os.chdir('..')
    print(f"{mainpid = }, going away for program restart")
    print("number of active children: " + str(len(mp.active_children())))   # side effect: joins finished tasks
    os.execl('./scraper_oo.py', './scraper_oo.py')


def reexec():
    global mainpid
    os.chdir('..')
    print(f"{mainpid = }, going away for program reexec")
    print("number of active children: " + str(len(mp.active_children())))   # side effect: joins finished tasks
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
    # In case we are called directly. Test program won't handle this properly.
    # signal.signal(signal.SIGUSR1, handle_special_signal)
    signal.signal(signal.SIGUSR2, handle_debug_signal)
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

            print("doing scrape task. date:", dt.datetime.now())
            main_scrape_task(context=context)

        except KeyError:
            print("warning: internal inconsistency! squashing KeyError exception... (alt main)", file=sys.stderr)
            traceback.print_exc()

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
    print(f"{mainpid = }. program modtime:", dt.datetime.fromtimestamp(os.stat(sys.argv[0]).st_mtime))

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

    nowtimestamp = str(get_timestamp_now())
    with open("discovery.txt", "a") as dlog:
        print("program started: " + nowtimestamp, file=dlog, flush=True)
        dlog.flush()
    global statuslog
    statuslog = open("status.txt", "a")
    print("program started: " + nowtimestamp, file=statuslog)
    statuslog.flush()
    os.fsync(statuslog.fileno())

    print("Updating lives status", flush=True)
    context.update_lives_status()

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
            dump_lives(context=context)
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
            traceback.print_exc()

        except KeyboardInterrupt:
            statuslog.flush()
            raise

        except ChatDownloaderError:
            print('warning: an error from chat_downloader was not handled! dumping backtrace.', file=sys.stderr)
            traceback.print_exc()

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during main loop processing") from exc

        finally:
            print_autoscraper_statistics(context=context)

            statuslog.flush()


def main_reexec_corruption_check(*, context):
    """ Reexec-specific corruption/crash check """
    def did_crash(video: Video):
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

        return crashed

    for video in context.lives.values():
        if video.progress == 'waiting':
            print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash: {video.progress} -> unscraped")
            video.reset_progress()

        elif video.progress == 'downloading':
            try:
                (pypid, dlpid) = context.pids[video.video_id]
                if not check_pid(dlpid):
                    # if the OS recycles PIDs, then this check might give bogus results. Obviously, don't 'reexec' after an OS reboot.
                    crashed = did_crash(video)

                    if crashed:
                        print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash (pid {dlpid}: check failed): {video.progress} -> unscraped")
                        video.reset_progress()

            except KeyError:
                print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash (pid unknown!): {video.progress} -> unscraped")
                video.reset_progress()

            except Exception:
                print(f"(initial check after reexec) video {video.video_id}: resetting progress after possible crash (exception!): {video.progress} -> unscraped")
                video.reset_progress()

        elif video.progress == 'downloaded':
            if video.status in ['prelive', 'live']:
                crashed = did_crash(video)
                if not crashed:
                    print(f'(initial check after reexec) video {video.video_id}: finished, moving status from {video.status} to postlive')
                    video.set_status('postlive')
                    if not has_metafile_status(video=video, status='postlive'):
                        print(f'warning: video {video.video_id} apparently finished but lacks postlive metafile; doing immediate rescrape')
                        video.rescrape_meta()
                        if video.status != 'error':
                            persist_basic_state(video, context=context, clobber=True)
                        persist_ytmeta(video, fresh=True, clobber=False)
                    else:
                        persist_basic_state(video, context=context, clobber=True)

        elif video.progress == 'aborted':
            if video.status in ['prelive', 'live', 'postlive']:
                if not has_metafile_status(video=video, status='postlive'):
                    print(f'warning: video {video.video_id} apparently aborted but lacks postlive metafile; doing immediate rescrape')
                    video.rescrape_meta()
                    if video.status != 'error' and video.meta:
                        persist_basic_state(video, context=context, clobber=True)
                        video.reset_progress()
                    else:
                        video.status = 'error'
                    persist_ytmeta(video, fresh=True, clobber=False)
                else:
                    persist_basic_state(video, context=context, clobber=True)


def main_initial_scrape_task(*, context):
    """ Task run before the main loop starts"""
    # Populate cache from disk
    for video_id, video in context.lives.items():
        progress = video.progress

        if progress == 'unscraped':
            # Try to load missing meta from disk
            recall_video(video_id, context=context, id_source='disk:initial_scrape_task')

    # There is a 4-hour explanation for this line, take a guess what happened.
    del video_id, video

    # Try to make sure downloaders are tracked with correct state
    process_dlpid_queue(context=context)

    # Scrape each video again if needed
    for video in context.lives.values():
        maybe_rescrape_initially(video, context=context)

    for video in context.lives.values():
        try:
            process_one_status(video, context=context, first=True)
        except ChatDownloaderError:
            print('warning: initial scrape task: process_one_status threw an error from chat_downloader.', file=sys.stderr)
        except OSError:
            print('error: initial scrape task: process_one_status threw an error from the OS.', file=sys.stderr)
            raise


def main_scrape_task(*, context):
    """ Task for each iteration of the main loop, without added delay. """
    context.update_lives_status()

    # Try to make sure downloaders are tracked with correct state
    process_dlpid_queue(context=context)

    # Scrape each video again if needed
    for video in context.lives.values():
        maybe_rescrape(video, context=context)

    for video_id in context.pids.copy():
        if video not in context.lives:
            recall_video(video_id, context=context, filter_progress=True, id_source='disk:scrape_task')
            if video.progress == 'waiting':
                try:
                    # This may modify our pid list, take care above.
                    try:
                        (pypid, dlpid) = context.pids[video.video_id]
                        if not check_pid(dlpid):
                            process_one_status(video, context=context, force=True)

                    except KeyError:
                        process_one_status(video, context=context, force=True)

                except ChatDownloaderError:
                    print('warning: main scrape task, pids check: process_one_status threw an error from chat_downloader.', file=sys.stderr)
                except OSError:
                    print('warning: main scrape task, pids check: process_one_status threw an error from the OS.', file=sys.stderr)

    for video in context.lives.values():
        # while there is a duplication of effort here, we need to advance the progress once somehow...
        if not video.did_progress_print or not video.did_status_print:
            try:
                process_one_status(video, context=context)
            except ChatDownloaderError:
                print('warning: main scrape task, progress check: process_one_status threw an error from chat_downloader.', file=sys.stderr)
            except OSError:
                print('warning: main scrape task, progress check: process_one_status threw an error from the OS.', file=sys.stderr)
        elif video.progress == 'downloading':
            try:
                (pypid, dlpid) = context.pids[video.video_id]
                if not check_pid(dlpid):
                    process_one_status(video, context=context, force=True)

            except ChatDownloaderError:
                print('warning: main scrape task, progress check invoking pid check: process_one_status threw an error from chat_downloader.', file=sys.stderr)

            except OSError:
                print('warning: main scrape task, progress check invoking pid check: process_one_status threw an error from the OS.', file=sys.stderr)

            except KeyError:
                if video.status == 'error':
                    print(f"(loop check) video {video.video_id}: pid unknown, and remote status reports error: {video.progress} -> aborted")
                    if video.progress not in ['downloaded', 'aborted']:
                        video.set_progress('aborted')
                elif video.status not in ['postlive', 'upload']:
                    print(f"(loop check) video {video.video_id}: resetting progress after possible corruption (pid unknown!): {video.progress} -> unscraped")
                    video.reset_progress()
                else:
                    if video.progress not in ['downloaded', 'aborted']:
                        try:
                            video.set_progress('downloaded')
                        except TransitionException:
                            print(f"(loop check) video {video.video_id}: pid unknown with status '{video.status}', cancelling download: {video.progress} -> aborted")
                            video.set_progress('aborted')

        if not video.did_meta_flush:
            print(f'warning: ... didn\'t flush meta.... flushing now... video: {video.video_id} (status {video.status}, progress {video.progress})')
            persist_meta(video=video, context=context, fresh=True, clobber=True)


def print_autoscraper_statistics(*, context: AutoScraper):
    print("number of active children: " + str(len(mp.active_children())))   # side effect: joins finished tasks
    print("number of known lives: " + str(len(context.lives)))

    counters: Dict[str, Any] = {'progress': {}, 'status': {}, 'diskmeta': 0, 'meta': 0, 'rawmeta': 0}
    for video in context.lives.values():
        counters['status'][video.status] = counters['status'].setdefault(video.status, 0) + 1
        counters['progress'][video.progress] = counters['progress'].setdefault(video.progress, 0) + 1
        counters['diskmeta'] += (video.meta is None and getattr(video, 'metafile_exists', False) is not False)
        counters['meta'] += (video.meta is not None)
        counters['rawmeta'] += (video.rawmeta is not None)

    print("video states:")
    for status, count in counters['status'].items():
        print(f"  number with video state {status}:", count)

    print("progress states:")
    for progress, count in counters['progress'].items():
        print(f"  number with progress state {progress}:", count)

    print("number of diskmeta objects:", counters['diskmeta'])
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
