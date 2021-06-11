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


downloadmetacmd="./yt-dlp/yt-dlp.sh -s -q -j --ignore-no-formats-error "
channelscrapecmd="./scrape_channel.sh"

def update_lives():
    os.system('wget -nv --load-cookies=cookies-schedule-hololive-tv.txt https://schedule.hololive.tv/lives -O auto-lives_tz')

    html_doc = ''
    with open("auto-lives_tz", "rb") as fp:
        html_doc = fp.read()

    soup = BeautifulSoup(html_doc, 'html.parser')
    with open("auto-lives_tz", "wb") as fp:
        fp.write(soup.prettify().encode())

    return soup


os.makedirs('by-video-id', exist_ok=True)
os.makedirs('chat-logs', exist_ok=True)
os.makedirs('pid', exist_ok=True)

lives_status = {}
cached_progress_status = {}
cached_ytmeta = {}
pids = {}

statuses = {'unknown', 'prelive', 'live', 'postlive', 'upload'}
progress_statuses = {'unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'}

# statuses:
# unknown: not yet scraped
# prelive: scheduled live
# live: in-progress live
# postlive: completed/missed live
# upload: not a livestream


# progress statuses:

# add -> unscraped
# unscraped -> waiting if scheduled
# unscraped -> downloading if downloader invoked
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

def update_lives_status():
    update_lives_status_holoschedule()
    update_lives_status_urllist()
    update_lives_status_channellist()


def update_lives_status_holoschedule():
    # Find all sections indicated by a 'day' header
    soup = update_lives()
    allcont = soup.find(id='all')
    allcontchildren = [node for node in allcont.children if len(repr(node)) > 4]
    localdate = ''
    for child in allcontchildren:
        day = child.find(class_='navbar-text')
        if day:
             localdate = [x for x in day.stripped_strings][0].split()[0]
        # Extract MM/DD date from header
        for link in child.find_all('a'):
            # Extract link
            href = link.get('href')
            if href.find('youtube') != -1:
                # Process Youtube link; get HH:MM starttime and user-friendly channel name (not localized)
                items = [x for x in link.stripped_strings]
                localtime = items[0]
                channelowner = items[1]
                safedatetime = localdate.replace('/', '-') + "T" + localtime.replace(':', '_')
                video_id = href[(href.find('v=') + 2):]
                if video_id not in lives_status.keys():
                    recall_meta(video_id)
                if video_id not in lives_status.keys():
                    lives_status[video_id] = 'unknown'
                    cached_progress_status[video_id] = 'unscraped'
                    print("discovery: new live listed: " + video_id + " " + localdate + " " + localtime + " : " + channelowner)
                else:
                    print("discovery: known live listed: " + video_id + " " + localdate + " " + localtime + " : " + channelowner)


def update_lives_status_urllist():
    pass #TODO


def update_lives_status_channellist():
    """ Read channels.txt for a list of channel IDs to process. """
    try:
        if os.path.exists("channels.txt"):
            with open("channels.txt") as channellist:
                for channel in [x.strip() for x in channellist.readlines()]:
                    invoke_channel_scraper(channel)
                    process_channel_videos(channel)
    except Exception:
        print("warning: unexpected error with processing channels.txt", file=sys.stderr)
        traceback.print_exc()


def invoke_channel_scraper(channel):
    """ Scrape the channel for latest videos and batch-fetch meta state. """
    # Note: some arbitrary limits are set in the helper program that may need tweaking.
    print("Scraping channel " + channel)
    os.system(channelscrapecmd + " " + channel)
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
            if video_id not in cached_ytmeta.keys():
                cached_ytmeta[video_id] = ytmeta
            else:
                print("ignoring ytmeta from channel scrape")


def process_channel_videos(channel):
    """ Read scraped channel video list, proccess each video ID, and persist the meta state. """
    try:
        print("Processing channel videos")
        with open("channel-cached/" + channel + ".url.all") as urls:
            for video_id in [f.split(" ")[1].strip() for f in urls.readlines()]:
                # Process each recent video
                if video_id not in lives_status.keys():
                    recall_meta(video_id)
                if video_id not in lives_status.keys():
                    lives_status[video_id] = 'unknown'
                    cached_progress_status[video_id] = 'unscraped'
                    print("discovery: new live listed: " + video_id + " on channel " + channel)
                else:
                    print("discovery: known live listed: " + video_id + " on channel " + channel)
                saved_progress = cached_progress_status[video_id]
                # process precached meta
                if video_id not in cached_ytmeta:
                    # We may be reloading old URLs after a program restart
                    print("ytmeta cache miss for video " + video_id + " on channel " + channel)
                    ytmeta = rescrape(video_id)
                    if ytmeta is None:
                        # scrape failed
                        continue
                    cached_ytmeta[video_id] = ytmeta
                process_ytmeta(video_id)
                # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
                # I look at this and am confused by the '==' here (and one place elsewhere)...
                if saved_progress not in {'missed', 'upload'} and saved_progress == cached_progress_status[video_id]:
                    persist_meta(video_id)
    except IOError:
        print("warning: unexpected I/O error when processing channel scrape results", file=sys.stderr)
        traceback.print_exc()


def persist_meta(video_id):
    if video_id not in lives_status.keys():
        raise ValueError('invalid video_id')
    metafile='by-video-id/' + video_id
    print('Updating ' + metafile)
    pidfile='pid/' + video_id
    meta = {}
    meta['status'] = lives_status[video_id]
    meta['progress'] = cached_progress_status[video_id]
    if video_id in cached_ytmeta:
        meta['ytmeta'] = cached_ytmeta[video_id] 
    with open(metafile, 'wb') as fp:
        fp.write(json.dumps(meta, indent=1).encode())
    with open(pidfile, 'wb') as fp:
        if pids.get(video_id) is not None:
            fp.write(str(pids[video_id][1]).encode()) # dlpid


def recall_meta(video_id):
    # Not cached in memory, look for saved state.
    metafile='by-video-id/' + video_id
    valid_meta = os.path.exists(metafile)
    meta = None
    if valid_meta:
        # Query saved state if not cached
        with open(metafile, 'rb') as fp:
            try:
                meta = json.loads(fp.read())
                valid_meta = meta['status'] in statuses and meta['progress'] in progress_statuses
            except json.decoder.JSONDecodeError:
                valid_meta = False
    if valid_meta:
        lives_status[video_id] = meta['status']
        cached_progress_status[video_id] = meta['progress']
        if 'ytmeta' in meta.keys():
            cached_ytmeta[video_id] = meta['ytmeta']


def process_ytmeta(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')
    if cached_ytmeta[video_id]['is_upcoming']:
        # note: premieres can also be upcoming but are not livestreams.
        lives_status[video_id] = 'prelive'
        if cached_progress_status[video_id] == 'unscraped':
            cached_progress_status[video_id] = 'waiting'
    elif cached_ytmeta[video_id]['is_live']:
        lives_status[video_id] = 'live'
        if cached_progress_status[video_id] == 'unscraped':
            cached_progress_status[video_id] = 'waiting'
    elif cached_ytmeta[video_id]['is_livestream'] or cached_ytmeta[video_id]['live_endtime']:
        # note: premieres also have a starttime and endtime
        lives_status[video_id] = 'postlive'
        if cached_progress_status[video_id] == 'unscraped':
            cached_progress_status[video_id] = 'missed'
    else:
        lives_status[video_id] = 'upload'
        cached_progress_status[video_id] = 'invalid'


def maybe_rescrape(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')
    saved_progress = cached_progress_status[video_id]
    if cached_progress_status[video_id] in {'unscraped', 'waiting'}:
        cached_progress_status[video_id] = 'unscraped'
    if cached_progress_status[video_id] == 'unscraped':
        ytmeta = rescrape(video_id)
        if ytmeta is None:
            # scrape failed
            return
        cached_ytmeta[video_id] = ytmeta
        process_ytmeta(video_id)
        # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
        if saved_progress not in {'missed', 'upload'} and saved_progress == cached_progress_status[video_id]:
            persist_meta(video_id)


def maybe_rescrape_initially(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')
    if cached_progress_status[video_id] in {'unscraped', 'waiting', 'downloading'}:
        cached_progress_status[video_id] = 'unscraped'
    if cached_progress_status[video_id] == 'unscraped':
        ytmeta = rescrape(video_id)
        if ytmeta is None:
            # scrape failed
            return
        cached_ytmeta[video_id] = ytmeta
        process_ytmeta(video_id)
    persist_meta(video_id)


def export_rescrape_fields(jsonres):
    ytmeta = {}
    ytmeta['raw'] = jsonres
    ytmeta['id'] = jsonres['id']
    ytmeta['title'] = jsonres['title']
    ytmeta['description'] = jsonres['description']
    ytmeta['duration'] = jsonres['duration']
    ytmeta['uploader'] = jsonres['uploader']
    ytmeta['channel_id'] = jsonres['channel_id']
    ytmeta['is_livestream'] = jsonres['was_live']
    ytmeta['is_live'] = jsonres['is_live']
    ytmeta['live_starttime'] = jsonres['live_starttime']
    ytmeta['live_endtime'] = jsonres['live_endtime']
    ytmeta['is_upcoming'] = jsonres['is_upcoming']
    return ytmeta

def rescrape(video_id):
    jsonres = invoke_scraper(video_id)
    if jsonres == None:
        # is here ok?
        cached_progress_status[video_id] = 'aborted'
        return None
    return export_rescrape_fields(jsonres)


def invoke_scraper(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')
    try:
        cmdline = downloadmetacmd + "-- " + video_id
        print(cmdline.split())
        proc = subprocess.run(cmdline.split(), capture_output=True)
        with open('outtmp', 'wb') as fp:
            fp.write(proc.stdout)
        if len(proc.stdout) == 0:
            print("scraper error: no stdout! stderr=" + proc.stderr)
            return None
        return json.loads(proc.stdout)
    except Exception as ex:
        print("warning: exception thrown during scrape task. printing traceback...", file=sys.stderr)
        traceback.print_exc()
        return None


def safen_path(s):
    try:
        return s.replace(':', '_').replace('/', '_').replace(' ', '_')[0:100]
    except Exception:
        print("warning: string safening failed, returning dummy value...")
        return ""


def get_outfile_basename(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')
    currtimesafe = "curr-" + safen_path(dt.datetime.utcnow().isoformat(timespec='seconds')) + "_UTC"
    try:
        #titlesafe = safen_path(cached_ytmeta[video_id]['title']) #breaks path limits
        uploadersafe = safen_path(cached_ytmeta[video_id]['uploader'])
        starttimesafe = "start-" + safen_path(cached_ytmeta[video_id]['live_starttime'])
        livestatus = lives_status[video_id]
        #basename = "_" + video_id + "_" + uploadersafe + "_" + titlesafe + "_" + livestatus + "_" + starttimesafe + "_" + currtimesafe
        basename = "_" + video_id + "_" + uploadersafe + "_" + livestatus + "_" + starttimesafe + "_" + currtimesafe
        return basename
    except Exception:
        print("warning: basename generation failed, using simpler name", file=sys.stderr)
        basename = "_" + video_id + "_" + currtimesafe
        return basename


q = mp.SimpleQueue()


def process_dlpid_queue():
    """ Process (empty) the queue of PIDs from newly invoked downloaders and update their state. """
    while not q.empty():
        (pid, dlpid, vid) = q.get()
        cached_progress_status[vid] = 'downloading'
        pids[vid] = (pid, dlpid)
        persist_meta(video_id)


def invoke_downloader(video_id):
    try:
        print('invoking for ' + str(video_id))
        if not lives_status.get(video_id):
            raise ValueError('invalid video_id')
        if pids.get(video_id):
            print("warning: duplicate invocation for video " + video_id + " (according to internal PID state)", file=sys.stderr)
        outfile = get_outfile_basename(video_id)
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


def _invoke_downloader_start(q, video_id, outfile):
    # There is not much use for the python pid, we store the process ID only for debugging
    pid = os.getpid()
    print("process fork " + str(pid) + " is live, with outfile " + outfile)
    proc = subprocess.Popen(['./downloader-invoker.sh', outfile, video_id])
    q.put((pid, proc.pid, video_id))
    q.close() # flush
    # Block (hopefully not the main process)
    proc.wait()
    print("process fork " + str(pid) + " has waited")


if __name__ == '__main__':
    #mp.set_start_method('forkserver')
    print("Updating lives status")
    update_lives_status()
    # Initial load
    print("Starting initial pass")
    if True:
        try:
            # Populate cache from disk
            for video_id in lives_status.keys():
                progress = cached_progress_status.get(video_id)
                if progress is None or progress == 'unscraped':
                    # Try to load missing meta from disk
                    recall_meta(video_id)
            # Try to make sure downloaders are tracked with correct state
            process_dlpid_queue()
            # Scrape each video again if needed
            for video_id in lives_status.keys():
                maybe_rescrape_initially(video_id)
            for video_id in lives_status.keys():
                if cached_progress_status[video_id] == 'waiting':
                    invoke_downloader(video_id)
                elif cached_progress_status[video_id] == 'missed': 
                    print("status: missed (possibly cached?): " + video_id)
                elif cached_progress_status[video_id] == 'invalid': 
                    print("status: upload (possibly cached/bogus?): " + video_id)
                elif cached_progress_status[video_id] == 'aborted': 
                    print("status: aborted (possibly cached/bogus?): " + video_id)
                elif cached_progress_status[video_id] == 'downloading': 
                    print("status: downloading (but this is wrong; we just started!): " + video_id)
                    if pids.get(video_id):
                        (pypid, dlpid) = pids[video_id]
                        if not check_pid(dlpid):
                            print("dlpid no longer exists: " + video_id)
                            cached_progress_status[video_id] = 'downloaded'
                            del pids[video_id]
                        else:
                            print("status: downloading (apparently, may be bogus): " + video_id)
                    else:
                        print("warning: pid lookup for video " + video_id + " failed (initial load, should be unreachable).", file=sys.stderr)
                elif cached_progress_status[video_id] == 'downloaded': 
                    print("status: finished (cached?): " + video_id)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            raise RuntimeError("Exception encountered during initial load processing") from exc
    print("Starting main loop")
    while True:
        try:
            time.sleep(300)
            update_lives_status()
            # Try to make sure downloaders are tracked with correct state
            process_dlpid_queue()
            # Scrape each video again if needed
            for video_id in lives_status.keys():
                maybe_rescrape(video_id)
            for video_id in lives_status.keys():
                if cached_progress_status[video_id] == 'waiting':
                    invoke_downloader(video_id)
                elif cached_progress_status[video_id] == 'missed': 
                    print("status: missed: " + video_id)
                elif cached_progress_status[video_id] == 'invalid': 
                    print("status: upload: " + video_id)
                elif cached_progress_status[video_id] == 'aborted': 
                    print("status: aborted: " + video_id)
                elif cached_progress_status[video_id] == 'downloading': 
                    #TODO: check PID/if process is running
                    if pids.get(video_id):
                        (pypid, dlpid) = pids[video_id]
                        if not check_pid(dlpid):
                            print("dlpid no longer exists: " + video_id)
                            cached_progress_status[video_id] = 'downloaded'
                            del pids[video_id]
                        else:
                            print("status: downloading: " + video_id)
                    else:
                        print("warning: pid lookup for video " + video_id + " failed.", file=sys.stderr)
                elif cached_progress_status[video_id] == 'downloaded': 
                    print("status: finished: " + video_id)
        except KeyError:
            print("warning: internal inconsistency! squashing KeyError exception...", file=sys.stderr)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            os.system('date')
            raise RuntimeError("Exception encountered during main loop processing") from exc
        finally:
            print("number of active children: " + str(len(mp.active_children()))) # side effect: joins finished tasks
            print("number of known lives: " + str(len(lives_status)))
            print("number of cached progress states: " + str(len(cached_progress_status)))
            print("number of cached ytmeta objects: " + str(len(cached_progress_status)))
            print("number of tracked pid groups: " + str(len(pids)))
            print(end='', flush=True)


