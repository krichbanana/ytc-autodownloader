#!/usr/bin/env python3

import datetime as dt
import os
import sys
import glob
import multiprocessing as mp
import subprocess
import time
import json


def extract_video_id_from_yturl(href, strict=False):
    """ Extract a Youtube video id from a given URL
        Accepts video ids, unless strict=True.
        Returns None on error or failure.
    """
    video_id = None

    try:
        start = -1
        if href.find('youtube.com/watch') != -1:
            start = href.find('v=') + 2
        elif href.find('youtu.be/') != -1:
            start = href.find('be/') + 3

        if start == -1:
            if len(href) == 11 and not strict:
                # Assume it's just the video id
                start = 0
            else:
                return None

        video_id = href[start:start + 11]

        return video_id

    except Exception:
        return None


def create_file_lock(file):
    """ Create the named file and do a blocking lock, writing the current PID
        and returning an int FD """
    pid = os.getpid()
    fd = os.open(file, os.O_CREAT | os.O_RDWR)
    # Block until locker has closed their FD or unlocked their file (possibly by dying)
    os.lockf(fd, os.F_LOCK, 0)
    os.ftruncate(fd, 0)
    os.write(fd, str(pid).encode())
    os.fsync(fd)
    return fd


def remove_file_lock(fd):
    """ Close the file and remove the file's lock """
    os.close(fd)


def _test_file_lock():
    start = time.perf_counter()
    file = "compress.test.lock"
    longtask = mp.Process(target=_locktasklong, args=[file])
    longtask.start()
    time.sleep(1)
    shorttask = mp.Process(target=_locktaskshort, args=[file])
    shorttask.start()
    longtask.join()
    shorttask.join()
    os.remove(file)
    assert(longtask.exitcode == 0)
    assert(shorttask.exitcode == 0)
    end = time.perf_counter()
    assert(end - start >= 4.0)


def _locktasklong(file):
    fd = create_file_lock(file)
    time.sleep(3)
    with open(file, "r") as fp:
        data = fp.read()
        try:
            assert data == str(os.getpid())
        except AssertionError:
            print(f"{data} != {os.getpid()}")
            raise
    remove_file_lock(fd)


def _locktaskshort(file):
    fd = create_file_lock(file)
    time.sleep(1)
    with open(file, "r") as fp:
        data = fp.read()
        try:
            assert data == str(os.getpid())
        except AssertionError:
            print(f"{data} != {os.getpid()}")
            raise
    remove_file_lock(fd)


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    else:
        return True


def get_utc_timestamp_now():
    # Gets a UTC timestamp, not an artificially shifted one without zone information
    return dt.datetime.now(tz=dt.timezone.utc).timestamp()


def get_timestamp_now():
    return dt.datetime.utcnow().timestamp()


_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def parse_timestamp_as_utc_datetime(ts):
    """ adds utc zone when parsing utc timestamp to avoid local tz conversion """
    timestamp = dt.datetime.utcfromtimestamp(ts).strftime(_TIME_FORMAT)
    return dt.datetime.strptime(timestamp + "+0000", _TIME_FORMAT + '%z')


def parse_iso8601_as_utc(text):
    """ adds utc zone when parsing utc iso8601 date to avoid local tz conversion """
    return float(dt.datetime.fromisoformat(text + '+00:00').timestamp())


def parse_iso8601_with_tz(text):
    """ Convert ISO-8601 formatted string to unix timestamp (with micros) """
    return float(dt.datetime.fromisoformat(text).timestamp())


def _parse_iso8601(text):
    """ Convert ISO-8601 formatted string to unix timestamp (with micros) """
    # likely not timezone-safe, do not use. use .timestamp() instead of .strftime(); strftime forces localtime.
    return float(dt.datetime.fromisoformat(text).strftime('%s.%f'))


def meta_load_loginfo_list(video_id):
    file = "by-video-id/" + str(video_id) + '.loginfo'
    if os.path.exists(file):
        try:
            blob = open(file).read()
            infolist = [x for x in json_stream_wrapper(blob)]
            if len(infolist) != 1:
                print(f'unusual loginfo: len {len(infolist)}', file=sys.stderr)
            return infolist
        except json.JSONDecodeError:
            print('json decode failed for loginfo', file=sys.stderr)
            return []
    else:
        print('(utils.py) could not find loginfo file:', file, file=sys.stderr)
        return []


def meta_load_fast(video_id):
    id_prefix = "by-video-id/" + str(video_id)
    if os.path.exists(id_prefix):
        meta = json.load(open(id_prefix + ".meta"))

        if 'ytmeta' not in meta:
            print('(utils.py) warning: could not find \'ytmeta\' key in meta', file=sys.stderr)

        return meta

    else:
        print('(utils.py) could not find status file:', id_prefix, file=sys.stderr)


def meta_load_all(video_id):
    id_prefix = "by-video-id/" + str(video_id)
    if os.path.exists(id_prefix):
        globber = id_prefix + '.meta*'
        for file in glob.iglob(globber):
            try:
                yield json.load(open(file))
            except json.JSONDecodeError:
                print('corrupt metafile json, ignoring', file=sys.stderr)
    else:
        print('(utils.py) could not find status file:', id_prefix, file=sys.stderr)


def meta_extract_field(meta, field):
    if callable(field):
        try:
            return field(meta)
        except (KeyError, AttributeError, ValueError, TypeError):
            return None
    else:
        return meta.get(field)


def _get_field_metafile(video_id, first_meta, keys, note=None):
    # .meta
    meta = first_meta or meta_load_fast(video_id)
    for key in keys:
        field = meta_extract_field(meta, key)
        if not note:
            note = key

        if field is not None:
            return field


def _get_field_status_metafiles(video_id, first_meta, keys, note=None):
    # .meta.status...
    for meta in meta_load_all(video_id):
        meta = meta_load_fast(video_id)
        for key in keys:
            field = meta_extract_field(meta, key)
            if field:
                return field


def _get_field_loginfo(video_id, first_meta, keys, note=None):
    # .loginfo
    for meta in meta_load_loginfo_list(video_id):
        try:
            for key in keys:
                field = meta_extract_field(meta, key)
                if field is not None:
                    return field
        except KeyError:
            pass
    return None


def get_field(video_id, first_meta, keys, note=None):
    # metakey, logkey
    if isinstance(keys, str) or callable(keys):
        keys = [keys, keys]
    field = None
    try:
        field = _get_field_loginfo(video_id, None, keys, note)
        if field is None:
            print(f'(utils.py) could not find {note} for video {video_id} in loginfo, trying metafile', file=sys.stderr)
            field = _get_field_metafile(video_id, first_meta, keys, note)
        else:
            return field

        if field is None:
            print(f'(utils.py) could not find {note} for video {video_id} in loginfo or metafile, trying alternative metafiles', file=sys.stderr)
            field = _get_field_status_metafiles(video_id, None, keys, note)
            if field is not None:
                return field
        else:
            return field

    except Exception:
        print(f'(utils.py) get_field failed for {len(keys)} keys', file=sys.stderr)
        raise

    if field is None:
        print(f'(utils.py) could not find {note} for video {video_id} anywhere', file=sys.stderr)

    return field


def get_channel_id(video_id, first_meta=None):
    return get_field(video_id, first_meta,
                     [lambda m: m['ytmeta']['channel_id'], 'channel_id', 'uploader_id'],
                     note='channel id')


def get_uploader(video_id, first_meta=None):
    return get_field(video_id, first_meta,
                     [lambda m: m['ytmeta']['uploader'], 'uploader', 'author'],
                     note='uploader/author')


def _parse_loginfo_currtime_iso(m):
    return int(parse_iso8601_with_tz(m['currtime_iso']))


def _parse_loginfo_currtime(m):
    return int(parse_iso8601_as_utc(m['currtime'][:-4].replace('_', ':')))


def get_curr_timestamp(video_id, first_meta=None):
    return get_field(video_id, first_meta,
                     [
                         lambda m: _parse_loginfo_currtime(m),
                         lambda m: _parse_loginfo_currtime_iso(m)
                     ],
                     note='curr timestamp')


def get_start_timestamp(video_id, first_meta=None):
    return get_field(video_id, first_meta,
                     [
                         lambda m: m['ytmeta']['live_starttime'], 'starttime',
                         lambda m: _parse_loginfo_currtime(m),
                         lambda m: _parse_loginfo_currtime_iso(m)
                     ],
                     note='start/curr timestamp')


def old_get_start_timestamp(video_id):
    try:
        meta = meta_load_fast(video_id)
        timestamp = meta_extract_start_timestamp(meta)

        if timestamp is None:
            print(f'(utils.py) could not find start timestamp for video {video_id}, trying loginfo', file=sys.stderr)
        else:
            return timestamp

        for meta in meta_load_loginfo_list(video_id):
            try:
                timestamp = meta['starttime']
                if timestamp:
                    break
            except KeyError:
                pass

        if timestamp is None:
            print(f'(utils.py) could not find start timestamp for video {video_id} in loginfo, trying alternative metafiles', file=sys.stderr)
        else:
            return timestamp

        for meta in meta_load_all(video_id):
            meta = meta_load_fast(video_id)
            timestamp = meta_extract_start_timestamp(meta)
            if timestamp:
                break

        if timestamp is None:
            print(f'(utils.py) could not find start timestamp for video {video_id} anywhere', file=sys.stderr)

        return timestamp

    except Exception:
        print('(utils.py) get_start_timestamp() failed', file=sys.stderr)
        raise

        return None


def meta_extract_start_timestamp(meta):
    try:
        return meta['ytmeta']['live_starttime']

    except Exception:
        return None


def meta_extract_end_timestamp(meta):
    try:
        return meta['ytmeta']['live_endtime']

    except Exception:
        return None


def meta_extract_raw_live_status(meta):
    try:
        return meta['ytmeta']['live_status']

    except Exception:
        return None


def meta_extract_duration(meta):
    try:
        return meta['ytmeta']['duration']

    except Exception:
        return None


def meta_extract_raw_live_latency_class(meta):
    """ Only provided by the chat_downloader scrape source """
    try:
        return meta['ytmeta']['raw']['videoDetails']['latencyClass']

    except Exception:
        return None


def json_stream_wrapper(blob: str):
    """ Convert an improperly stored multi-json string into a generator list """
    try:
        yield json.loads(blob)
    except json.JSONDecodeError as e:
        error = True
        while error:
            yield json.loads(e.doc[0:e.pos])
            try:
                yield json.loads(e.doc[e.pos:])
                error = False
            except json.JSONDecodeError as e2:
                error = True
                if e2.pos == 0:
                    print('json stream loop: no progress', file=sys.stderr)
                    raise
                e = e2


def get_commit():
    try:
        proc = subprocess.Popen(['git', 'describe', '--long', '--always'], stdout=subprocess.PIPE)
        proc.wait()
        return proc.stdout.read().decode().strip()
    except OSError:
        return ''


if __name__ == '__main__':
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?v=z80mWoPiZUc") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?v=z80mWoPiZUc?t=1s") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?t=1s&v=z80mWoPiZUc&feature=youtu.be") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://youtu.be/z80mWoPiZUc") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://youtu.be/z80mWoPiZUc?t=1s") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("z80mWoPiZUc") == "z80mWoPiZUc"
    assert get_timestamp_now() is not None
    _test_file_lock()
    assert check_pid(os.getpid())
    if __debug__:
        print("tests passed")
