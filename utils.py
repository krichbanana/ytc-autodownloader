#!/usr/bin/env python3

def extract_video_id_from_yturl(href):
    """ Extract a Youtube video id from a given URL
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
            return None

        video_id = href[start:start+11]

        return video_id

    except Exception:
        return None


if __name__ == '__main__':
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?v=z80mWoPiZUc") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?v=z80mWoPiZUc?t=1s") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://www.youtube.com/watch?t=1s&v=z80mWoPiZUc&feature=youtu.be") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://youtu.be/z80mWoPiZUc") == "z80mWoPiZUc"
    assert extract_video_id_from_yturl("https://youtu.be/z80mWoPiZUc?t=1s") == "z80mWoPiZUc"
    if __debug__:
        print("tests passed")
