#!/usr/bin/env python3

import os
import multiprocessing as mp
import time


def extract_video_id_from_yturl(href):
    """ Extract a Youtube video id from a given URL
        Accepts video ids.
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
            if len(href) == 11:
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


if __name__ == '__main__':
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?v=z80mWoPiZUc") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?v=z80mWoPiZUc?t=1s") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?t=1s&v=z80mWoPiZUc&feature=youtu.be") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://youtu.be/z80mWoPiZUc") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://youtu.be/z80mWoPiZUc?t=1s") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("z80mWoPiZUc") == "z80mWoPiZUc"
    _test_file_lock()
    assert check_pid(os.getpid())
    if __debug__:
        print("tests passed")
