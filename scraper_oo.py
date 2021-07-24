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


# Debug switch
DISABLE_PERSISTENCE = False
FORCE_RESCRAPE = False

downloadmetacmd = "../yt-dlp/yt-dlp.sh -s -q -j --ignore-no-formats-error "
downloadchatprgm = "../downloader.py"
channelscrapecmd = "../scrape_channel.sh"
channelsfile = "./channels.txt"
watchdogprog = "../watchdog.sh"
holoscrapecmd = 'wget -nv --load-cookies=../cookies-schedule-hololive-tv.txt https://schedule.hololive.tv/lives -O auto-lives_tz'

# dict: video_id => Video
lives = {}
pids = {}
general_stats = {}  # for debugging

statuses = {'unknown', 'prelive', 'live', 'postlive', 'upload'}
progress_statuses = {'unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'}


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
        self.init_timestamp = dt.datetime.utcnow().timestamp()
        self.transition_timestamp = self.init_timestamp
        self.meta_timestamp = None
        # might delete one
        self.meta = None
        self.rawmeta = None
        # might change
        self.did_status_print = False
        self.did_progress_print = False
        self.did_discovery_print = False

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

        self.transition_timestamp = dt.datetime.utcnow().timestamp()
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

        self.transition_timestamp = dt.datetime.utcnow().timestamp()
        self.progress = progress

    def reset_status(self):
        """ Set the status to 'unknown'. Useful for clearing state loaded from disk. """
        self.status = 'unknown'

    def reset_progress(self):
        """ Set progress to 'unscraped'. Useful for clearing state loaded from disk. """
        self.progress = 'unscraped'

    def prepare_meta(self):
        """ Load meta from disk or fetch it from YouTube. """
        if self.meta is None:
            self.meta = rescrape(self)
            self.rawmeta = self.meta.get('raw')
            if self.rawmeta:
                del self.meta['raw']

            self.meta_timestamp = dt.datetime.utcnow().timestamp()

    def rescrape_meta(self):
        """ Ignore known meta and fetch meta from YouTube. """
        meta = rescrape(self)
        if meta:
            self.meta = meta
            self.rawmeta = self.meta.get('raw')
            if self.rawmeta:
                del self.meta['raw']

            self.meta_timestamp = dt.datetime.utcnow().timestamp()


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


def extract_video_id_from_yturl(href):
    """ Extract a Youtube video id from a given URL
        Returns None on error or failure.
    """
    video_id = None

    try:
        if href.find('youtube.com/watch') != -1:
            video_id = href[(href.find('v=') + 2):]
        elif href.find('youtu.be/') != -1:
            video_id = href[(href.find('be/') + 3):]

        if len(video_id) < 2:
            return None

        return video_id

    except Exception:
        return None


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
                for channel in [x.strip() for x in channellist.readlines()]:
                    invoke_channel_scraper(channel)
                    process_channel_videos(channel, dlog)

    except Exception:
        print("warning: unexpected error with processing channels.txt", file=sys.stderr)
        traceback.print_exc()


def invoke_channel_scraper(channel):
    """ Scrape the channel for latest videos and batch-fetch meta state. """
    # Note: some arbitrary limits are set in the helper program that may need tweaking.
    print("Scraping channel " + channel)
    subprocess.run(channelscrapecmd + " " + channel, shell=True)

    print("Processing channel metadata")
    with open("channel-cached/" + channel + ".meta.new") as allmeta:
        metalist = []

        for jsonres in allmeta.readlines():
            try:
                metalist.append(export_rescrape_fields(json.loads(jsonres)))
            except Exception:
                print("warning: exception in channel scrape task (corrupt meta?)", file=sys.stderr)
                traceback.print_exc()

        for ytmeta in metalist:
            video_id = ytmeta["id"]
            video = lives.get(video_id)
            if video and video.meta is None:
                video.meta = ytmeta
                video.rawmeta = ytmeta.get('raw')
            else:
                print("ignoring ytmeta from channel scrape")


# TODO: rewrite
def process_channel_videos(channel, dlog):
    """ Read scraped channel video list, proccess each video ID, and persist the meta state. """
    newlives = 0
    knownlives = 0
    numignores = {}

    try:
        print("Processing channel videos")
        with open("channel-cached/" + channel + ".url.all") as urls:
            for video_id in [f.split(" ")[1].strip() for f in urls.readlines()]:
                # Process each recent video
                if video_id not in lives:
                    recall_video(video_id, filter_progress=True)
                    video = lives[video_id]

                if video_id not in lives:
                    video = Video(video_id)
                    lives[video_id] = video
                    print("discovery: new live listed: " + video_id + " on channel " + channel, file=dlog, flush=True)
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
                    print("ytmeta cache miss for video " + video_id + " on channel " + channel)
                    cache_miss = True
                    video.meta = rescrape(video_id)
                    if video.meta is None:
                        # scrape failed
                        continue
                    video.rawmeta = video.meta.get('raw')

                process_ytmeta(video)

                # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
                # I look at this and am confused by the '==' here (and one place elsewhere)...
                if cache_miss or (saved_progress not in {'missed', 'invalid'} and saved_progress != video.progress):
                    persist_meta(video, fresh=True)

    except IOError:
        print("warning: unexpected I/O error when processing channel scrape results", file=sys.stderr)
        traceback.print_exc()

    print("discovery: channels list: new lives on channel " + channel + " : " + str(newlives))
    print("discovery: channels list: known lives on channel " + channel + " : " + str(knownlives))
    for progress, count in numignores.items():
        print("discovery: channels list: skipped ytmeta fetches on channel " + channel + " : " + str(count) + " skipped due to progress state '" + progress + "'")


def persist_meta(video: Video, fresh=False):
    video_id = video.video_id

    metafile = 'by-video-id/' + video_id

    # Debug switch
    if DISABLE_PERSISTENCE:
        print('NOT updating ' + metafile)
        return

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
        metafileyt = metafile + ".meta"
        metafileyt_status = metafileyt + "." + video.status

        print('Updating ' + metafileyt)
        with open(metafileyt, 'wb') as fp:
            fp.write(json.dumps(ytmeta, indent=1).encode())
        print('Updating ' + metafileyt_status)
        with open(metafileyt_status, 'wb') as fp:
            fp.write(json.dumps(ytmeta, indent=1).encode())

    with open(metafile, 'wb') as fp:
        fp.write(json.dumps(meta, indent=1).encode())

    with open(pidfile, 'wb') as fp:
        if pids.get(video_id) is not None:
            # Write dlpid to file
            fp.write(str(pids[video_id][1]).encode())


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

        # unmigrated (monolithic file) format
        elif 'ytmeta' in meta:
            video.meta = ytmeta['ytmeta']
            video.rawmeta = ytmeta['ytmeta'].get('raw')

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
    if video.progress in {'unscraped', 'waiting'}:
        video.reset_progress()

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
    if video.progress in {'unscraped', 'waiting', 'downloading'}:
        video.reset_progress()

    if video.progress == 'unscraped' or FORCE_RESCRAPE:
        video.rescrape_meta()
        if video.meta is None:
            # scrape failed
            return

        process_ytmeta(video)

    # Redundant, but purges corruption
    persist_meta(video, fresh=True)


def export_rescrape_fields(jsonres):
    ytmeta = {}
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

        except KeyError:
            print("warning: exporting ytmeta fields not fully successful, expect this download to fail", file=sys.stderr)
            ytmeta['is_livestream'] = ytmeta.get('is_livestream')
            ytmeta['live_starttime'] = ytmeta.get('live_starttime')
            ytmeta['live_endtime'] = ytmeta.get('live_endtime')
            ytmeta['live_status'] = ytmeta.get('live_status')
            ytmeta['is_live'] = ytmeta.get('is_live')
            # last-ditch effort to avoid missing a stream
            ytmeta['is_upcoming'] = ytmeta.get('is_upcoming') or not bool(ytmeta['duration'])

    return ytmeta


def rescrape(video: Video):
    """ Invoke the scraper on a video now. """
    jsonres = invoke_scraper(video.video_id)
    if jsonres is None:
        # Mark as aborted here, before processing
        video.set_progress('aborted')

        return None

    return export_rescrape_fields(jsonres)


def invoke_scraper(video_id):
    if video_id not in lives:
        raise ValueError('invalid video_id')

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
        channel_id = video.meta.get('live_starttime')
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
    q.close()
    # Block this fork (hopefully not the main process)
    proc.wait()
    print("process fork " + str(pid) + " has waited")


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
        print("status: just invoked: " + video_id, file=statuslog)
        if video.meta is None:
            print("warning: video.meta missing for video " + video_id, file=sys.stderr)

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

        if pids.get(video_id):
            (pypid, dlpid) = pids[video_id]

            if not check_pid(dlpid):
                print("status: dlpid no longer exists: " + video_id, file=statuslog)
                video.set_progress('downloaded')

                del pids[video_id]
                persist_meta(video, fresh=True)

                delete_ytmeta_raw(video)

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

    elif video.progress == 'downloaded':
        if first:
            print("status: finished (cached?): " + video_id, file=statuslog)
        else:
            print("status: finished: " + video_id, file=statuslog)

            delete_ytmeta_raw(video)

    video.did_progress_print = True
    statuslog.flush()


def handle_special_signal(signum, frame):
    os.makedirs('dump', exist_ok=True)

    with open("dump/lives", "w") as fp:
        fp.write(json.dumps(lives))

    with open("dump/pids", "w") as fp:
        fp.write(json.dumps(pids))

    with open("dump/general_stats", "w") as fp:
        fp.write(json.dumps(general_stats))

    with open("dump/staticconfig", "w") as fp:
        print("FORCE_RESCRAPE=" + str(FORCE_RESCRAPE), file=fp)
        print("DISABLE_PERSISTENCE=" + str(DISABLE_PERSISTENCE), file=fp)


# TODO
if __name__ == '__main__':
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
    statuslog = open("status.txt", "a")
    print("program started", file=statuslog)

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
            raise

        except Exception as exc:
            start_watchdog()
            raise RuntimeError("Exception encountered during initial load processing") from exc

    statuslog.flush()

    print("Starting main loop", flush=True)
    while True:
        try:
            time.sleep(300)
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
