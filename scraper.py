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

downloadmetacmd = "./yt-dlp/yt-dlp.sh -s -q -j --ignore-no-formats-error "
channelscrapecmd = "./scrape_channel.sh"

lives_status = {}
saved_progress_status = {}
fresh_progress_status = {}  # migrates to saved after statuses are reported
cached_ytmeta = {}
pids = {}

statuses = {'unknown', 'prelive', 'live', 'postlive', 'upload'}
progress_statuses = {'unscraped', 'waiting', 'downloading', 'downloaded', 'missed', 'invalid', 'aborted'}

# video statuses:
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
    os.system('wget -nv --load-cookies=cookies-schedule-hololive-tv.txt https://schedule.hololive.tv/lives -O auto-lives_tz')

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
                video_id = href[(href.find('v=') + 2):]

                if video_id not in lives_status.keys():
                    recall_meta(video_id, filter_progress=True)

                if video_id not in lives_status.keys():
                    lives_status[video_id] = 'unknown'
                    fresh_progress_status[video_id] = 'unscraped'
                    print("discovery: new live listed: " + video_id + " " + localdate + " " + localtime + " : " + channelowner, file=dlog, flush=True)
                    newlives += 1
                else:
                    # known (not new) live listed
                    knownlives += 1

    print("discovery: holoschedule: new lives: " + str(newlives))
    print("discovery: holoschedule: known lives: " + str(knownlives))


def update_lives_status_urllist(dlog):
    # TODO
    pass


def update_lives_status_channellist(dlog):
    """ Read channels.txt for a list of channel IDs to process. """
    try:
        if os.path.exists("channels.txt"):
            with open("channels.txt") as channellist:
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
                if video_id not in lives_status.keys():
                    recall_meta(video_id, filter_progress=True)

                if video_id not in lives_status.keys():
                    lives_status[video_id] = 'unknown'
                    fresh_progress_status[video_id] = 'unscraped'
                    print("discovery: new live listed: " + video_id + " on channel " + channel, file=dlog, flush=True)
                    newlives += 1
                else:
                    # known (not new) live listed (channel unaware)
                    knownlives += 1

                saved_progress = fresh_progress_status[video_id]

                if not FORCE_RESCRAPE and saved_progress in {'downloaded', 'missed', 'invalid', 'aborted'}:
                    numignores[saved_progress] = numignores.setdefault(saved_progress, 0) + 1

                    try:
                        del cached_ytmeta[video_id]
                    except KeyError:
                        pass

                    continue

                cache_miss = False

                # process precached meta
                if video_id not in cached_ytmeta:
                    # We may be reloading old URLs after a program restart
                    print("ytmeta cache miss for video " + video_id + " on channel " + channel)
                    cache_miss = True
                    ytmeta = rescrape(video_id)
                    if ytmeta is None:
                        # scrape failed
                        continue
                    cached_ytmeta[video_id] = ytmeta

                process_ytmeta(video_id)

                # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
                # I look at this and am confused by the '==' here (and one place elsewhere)...
                if cache_miss or (saved_progress not in {'missed', 'invalid'} and saved_progress != fresh_progress_status[video_id]):
                    persist_meta(video_id, fresh=True)

    except IOError:
        print("warning: unexpected I/O error when processing channel scrape results", file=sys.stderr)
        traceback.print_exc()

    print("discovery: channels list: new lives on channel " + channel + " : " + str(newlives))
    print("discovery: channels list: known lives on channel " + channel + " : " + str(knownlives))
    for progress, count in numignores.items():
        print("discovery: channels list: skipped ytmeta fetches on channel " + channel + " : " + str(count) + " skipped due to progress state '" + progress + "'")


def persist_meta(video_id, fresh=False):
    if video_id not in lives_status.keys():
        raise ValueError('invalid video_id')

    metafile = 'by-video-id/' + video_id

    # Debug switch
    if DISABLE_PERSISTENCE:
        print('NOT updating ' + metafile)
        return

    print('Updating ' + metafile)
    pidfile = 'pid/' + video_id
    meta = {}
    meta['status'] = lives_status[video_id]

    # try to use the copy; better to retry than miss downloading
    if (saved_progress_status.get(video_id) is None) or fresh:
        meta['progress'] = fresh_progress_status[video_id]
    else:
        meta['progress'] = saved_progress_status[video_id]

    # Write ytmeta to a separate file (to avoid slurping large amounts of data)
    if video_id in cached_ytmeta:
        ytmeta = {}
        ytmeta['ytmeta'] = cached_ytmeta[video_id]
        metafileyt = metafile + ".meta"
        metafileyt_status = metafileyt + "." + lives_status[video_id]

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


def recall_meta(video_id, filter_progress=False):
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
            should_ignore = meta['progress'] in {'unscraped', 'waiting'}

        # note: FORCE_RESCRAPE might clobber old ytmeta if not loaded (bad if the video drastically changes or goes unavailable)
        if valid_ytmeta and not should_ignore:
            with open(metafileyt, 'rb') as fp:
                try:
                    ytmeta = json.loads(fp.read())
                    valid_ytmeta = 'ytmeta' in ytmeta

                except (json.decoder.JSONDecodeError, KeyError):
                    valid_ytmeta = False

    if valid_meta:
        # Commit status to runtime tracking (else we would discard it here)
        lives_status[video_id] = meta['status']
        fresh_progress_status[video_id] = meta['progress']

        if valid_ytmeta and not should_ignore:
            cached_ytmeta[video_id] = ytmeta['ytmeta']

        # unmigrated (monolithic file) format
        elif 'ytmeta' in meta.keys():
            cached_ytmeta[video_id] = meta['ytmeta']

            if DISABLE_PERSISTENCE:
                return

            print('notice: migrating ytmeta in status file to new file right now: ' + metafile)
            persist_meta(video_id, fresh=True)

            if should_ignore:
                try:
                    del cached_ytmeta[video_id]
                except KeyError:
                    pass


def process_ytmeta(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')
    if video_id not in cached_ytmeta.keys():
        raise RuntimeError('precondition failed: called process_ytmeta but ytmeta for video_id ' + video_id + ' not found.')

    if cached_ytmeta[video_id]['is_upcoming']:
        # note: premieres can also be upcoming but are not livestreams.
        lives_status[video_id] = 'prelive'
        if fresh_progress_status[video_id] == 'unscraped':
            fresh_progress_status[video_id] = 'waiting'

    elif cached_ytmeta[video_id]['is_live']:
        lives_status[video_id] = 'live'
        if fresh_progress_status[video_id] == 'unscraped':
            fresh_progress_status[video_id] = 'waiting'

    elif cached_ytmeta[video_id]['is_livestream'] or cached_ytmeta[video_id]['live_endtime']:
        # note: premieres also have a starttime and endtime
        lives_status[video_id] = 'postlive'
        if fresh_progress_status[video_id] == 'unscraped':
            fresh_progress_status[video_id] = 'missed'

    else:
        lives_status[video_id] = 'upload'
        fresh_progress_status[video_id] = 'invalid'


def maybe_rescrape(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')

    saved_progress = fresh_progress_status[video_id]
    if fresh_progress_status[video_id] in {'unscraped', 'waiting'}:
        fresh_progress_status[video_id] = 'unscraped'

    if fresh_progress_status[video_id] == 'unscraped':
        ytmeta = rescrape(video_id)
        if ytmeta is None:
            # scrape failed
            return

        cached_ytmeta[video_id] = ytmeta
        process_ytmeta(video_id)

        # Avoid redundant disk flushes (as long as we presume that the title/description/listing status won't change)
        if saved_progress not in {'missed', 'invalid'} and saved_progress != fresh_progress_status[video_id]:
            persist_meta(video_id, fresh=True)


def maybe_rescrape_initially(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')

    if fresh_progress_status[video_id] in {'unscraped', 'waiting', 'downloading'}:
        fresh_progress_status[video_id] = 'unscraped'

    if fresh_progress_status[video_id] == 'unscraped' or FORCE_RESCRAPE:
        ytmeta = rescrape(video_id)
        if ytmeta is None:
            # scrape failed
            return

        cached_ytmeta[video_id] = ytmeta
        process_ytmeta(video_id)

    # Redundant, but purges corruption
    persist_meta(video_id, fresh=True)


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
    """ Invoke the scraper on a video now. """
    jsonres = invoke_scraper(video_id)
    if jsonres is None:
        # Mark as aborted here, before processing
        fresh_progress_status[video_id] = 'aborted'

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
        return s.replace(':', '_').replace('/', '_').replace(' ', '_')[0:100]

    except Exception:
        print("warning: string safening failed, returning dummy value...")

        return ""


def get_outfile_basename(video_id):
    if not lives_status.get(video_id):
        raise ValueError('invalid video_id')

    currtimesafe = "curr-" + safen_path(dt.datetime.utcnow().isoformat(timespec='seconds')) + "_UTC"

    try:
        # We could embed the title here, but tracking path limits is a pain, and the title might be mangled
        uploadersafe = safen_path(cached_ytmeta[video_id]['uploader'])
        starttimesafe = "start-" + safen_path(cached_ytmeta[video_id]['live_starttime'])
        livestatus = lives_status[video_id]
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
        fresh_progress_status[vid] = 'downloading'
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


def start_watchdog():
    """ Ensure the program exits after a top-level exception. """
    os.system('date')
    subprocess.Popen(["./watchdog.sh", str(os.getpid())])


def _invoke_downloader_start(q, video_id, outfile):
    # There is not much use for the python pid, we store the process ID only for debugging
    pid = os.getpid()
    print("process fork " + str(pid) + " is live, with outfile " + outfile)
    proc = subprocess.Popen(['./downloader-invoker.sh', outfile, video_id])

    q.put((pid, proc.pid, video_id))
    # Close the queue to flush it and avoid blocking the python process on exit.
    q.close()
    # Block this fork (hopefully not the main process)
    proc.wait()
    print("process fork " + str(pid) + " has waited")


def process_one_status(video_id, first=False):
    # Process only on change
    if fresh_progress_status[video_id] == saved_progress_status.get(video_id):
        return

    if fresh_progress_status[video_id] == 'waiting':
        print("status: just invoked: " + video_id, file=statuslog)
        if video_id not in cached_ytmeta.keys():
            print("warning: cached_ytmeta missing for video " + video_id, file=sys.stderr)

        invoke_downloader(video_id)

    elif fresh_progress_status[video_id] == 'missed':
        if first:
            print("status: missed (possibly cached?): " + video_id, file=statuslog)
        else:
            print("status: missed: " + video_id, file=statuslog)

        try:
            del cached_ytmeta[video_id]['raw']
        except KeyError:
            pass

    elif fresh_progress_status[video_id] == 'invalid':
        if first:
            print("status: upload (possibly cached/bogus?): " + video_id, file=statuslog)
        else:
            print("status: upload: " + video_id, file=statuslog)

        try:
            del cached_ytmeta[video_id]['raw']
        except KeyError:
            pass

    elif fresh_progress_status[video_id] == 'aborted':
        if first:
            print("status: aborted (possibly cached/bogus?): " + video_id, file=statuslog)
        else:
            print("status: aborted: " + video_id, file=statuslog)

        try:
            del cached_ytmeta[video_id]['raw']
        except KeyError:
            pass

    elif fresh_progress_status[video_id] == 'downloading':
        if first:
            print("status: downloading (but this is wrong; we just started!): " + video_id, file=statuslog)

        if pids.get(video_id):
            (pypid, dlpid) = pids[video_id]

            if not check_pid(dlpid):
                print("status: dlpid no longer exists: " + video_id, file=statuslog)
                fresh_progress_status[video_id] = 'downloaded'

                del pids[video_id]
                persist_meta(video_id, fresh=True)

                try:
                    del cached_ytmeta[video_id]['raw']
                except KeyError:
                    pass

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

    elif fresh_progress_status[video_id] == 'downloaded':
        if first:
            print("status: finished (cached?): " + video_id, file=statuslog)
        else:
            print("status: finished: " + video_id, file=statuslog)

            try:
                del cached_ytmeta[video_id]['raw']
            except KeyError:
                pass

    saved_progress_status[video_id] = fresh_progress_status[video_id]
    statuslog.flush()


def handle_special_signal(signum, frame):
    os.makedirs('dump', exist_ok=True)

    with open("dump/lives_status", "w") as fp:
        fp.write(json.dumps(lives_status).encode())

    with open("dump/saved_progress_status", "w") as fp:
        fp.write(json.dumps(saved_progress_status).encode())

    with open("dump/fresh_progress_status", "w") as fp:
        fp.write(json.dumps(fresh_progress_status).encode())

    with open("dump/cached_ytmeta", "w") as fp:
        fp.write(json.dumps(cached_ytmeta).encode())

    with open("dump/pids", "w") as fp:
        fp.write(json.dumps(pids).encode())

    with open("dump/staticconfig", "w") as fp:
        print("FORCE_RESCRAPE=" + str(FORCE_RESCRAPE), file=fp)
        print("DISABLE_PERSISTENCE=" + str(DISABLE_PERSISTENCE), file=fp)


if __name__ == '__main__':
    # Prep storage and persistent state directories
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
            for video_id in lives_status.keys():
                progress = fresh_progress_status.get(video_id)

                if progress is None or progress == 'unscraped':
                    # Try to load missing meta from disk
                    recall_meta(video_id)

            # Try to make sure downloaders are tracked with correct state
            process_dlpid_queue()

            # Scrape each video again if needed
            for video_id in lives_status.keys():
                maybe_rescrape_initially(video_id)

            for video_id in lives_status.keys():
                process_one_status(video_id, first=True)

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
            for video_id in lives_status.keys():
                maybe_rescrape(video_id)

            for video_id in lives_status.keys():
                process_one_status(video_id)

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
            print("number of known lives: " + str(len(lives_status)))
            print("number of saved progress states: " + str(len(saved_progress_status)))
            print("number of cached ytmeta objects: " + str(len(cached_ytmeta)))
            print("number of tracked pid groups: " + str(len(pids)))
            print(end='', flush=True)
            statuslog.flush()
