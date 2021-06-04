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
                    print("new live listed: " + video_id + " " + localdate + " " + localtime + " : " + channelowner)
                else:
                    print("known live listed: " + video_id + " " + localdate + " " + localtime + " : " + channelowner)


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
            except JSONDecodeError:
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


def rescrape(video_id):
    jsonres = invoke_scraper(video_id)
    if jsonres == None:
        # is here ok?
        cached_progress_status[video_id] = 'aborted'
        return None
    ytmeta = {}
    ytmeta['raw'] = jsonres
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
        print("warning: exception thrown during scrape task. printing traceback...")
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
        print("warning: basename generation failed, using simpler name")
        basename = "_" + video_id + "_" + currtimesafe
        return basename


q = mp.SimpleQueue()


def invoke_downloader(video_id):
    #print('FAKED: invoking for ' + str(video_id))
    #return None #TEMP XXX
    try:
        print('invoking for ' + str(video_id))
        if not lives_status.get(video_id):
            raise ValueError('invalid video_id')
        outfile = get_outfile_basename(video_id)
        p = mp.Process(target=_invoke_downloader_start, args=(q, video_id, outfile))
        p.start()
        time.sleep(0.5) # wait for spawn
        while not q.empty():
            (pid, dlpid, vid) = q.get()
            cached_progress_status[vid] = 'downloading'
            pids[vid] = (pid, dlpid)
        persist_meta(video_id)
        time.sleep(0.5)
    except Exception:
        print("warning: downloader invocation failed because of an exception. printing traceback...")
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
    update_lives_status()
    # Initial load
    if True:
        try:
            # Populate cache from disk
            for video_id in lives_status.keys():
                progress = cached_progress_status.get(video_id)
                if progress is None or progress == 'unscraped':
                    # Try to load missing meta from disk
                    recall_meta(video_id)
            # Scrape each video again if needed
            for video_id in lives_status.keys():
                maybe_rescrape_initially(video_id)
            for video_id in lives_status.keys():
                if cached_progress_status[video_id] == 'waiting':
                    invoke_downloader(video_id)
                elif cached_progress_status[video_id] == 'missed': 
                    print("missed (possibly cached?): " + video_id)
                elif cached_progress_status[video_id] == 'invalid': 
                    print("upload (possibly cached/bogus?): " + video_id)
                elif cached_progress_status[video_id] == 'aborted': 
                    print("aborted (possibly cached/bogus?): " + video_id)
                elif cached_progress_status[video_id] == 'downloading': 
                    print("downloading (but this is wrong; we just started!): " + video_id)
                    if pids.get(video_id):
                        (pypid, dlpid) = pids[video_id]
                        if not check_pid(dlpid):
                            print("dlpid no longer exists: " + video_id)
                            cached_progress_status[video_id] = 'downloaded'
                            if check_pid(pypid):
                                print("warning: pypid still exists! (" + str(pypid) + " ): " + video_id )
                        else:
                            print("downloading (apparently, may be bogus): " + video_id)
                    else:
                        print("warning: pid lookup for video " + video_id + " failed (initial load, should be unreachable).")
                elif cached_progress_status[video_id] == 'downloaded': 
                    print("finished (cached?): " + video_id)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            raise RuntimeError("Exception encountered during initial load processing") from exc
    while True:
        try:
            time.sleep(300)
            update_lives_status()
            # Scrape each video again if needed
            for video_id in lives_status.keys():
                maybe_rescrape(video_id)
            for video_id in lives_status.keys():
                if cached_progress_status[video_id] == 'waiting':
                    invoke_downloader(video_id)
                elif cached_progress_status[video_id] == 'missed': 
                    print("missed: " + video_id)
                elif cached_progress_status[video_id] == 'invalid': 
                    print("upload: " + video_id)
                elif cached_progress_status[video_id] == 'aborted': 
                    print("aborted: " + video_id)
                elif cached_progress_status[video_id] == 'downloading': 
                    #TODO: check PID/if process is running
                    if pids.get(video_id):
                        (pypid, dlpid) = pids[video_id]
                        if not check_pid(dlpid):
                            print("dlpid no longer exists: " + video_id)
                            cached_progress_status[video_id] = 'downloaded'
                            if check_pid(pypid):
                                print("warning: pypid still exists! (" + str(pypid) + " ): " + video_id )
                        else:
                            print("downloading: " + video_id)
                    else:
                        print("warning: pid lookup for video " + video_id + " failed.")
                elif cached_progress_status[video_id] == 'downloaded': 
                    print("finished: " + video_id)
        except KeyError:
            print("warning: internal inconsistency! squashing KeyError exception...")
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


