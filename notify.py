#!/usr/bin/env python3
import subprocess


enable = True
warn = True


def htmlescape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# Linux only
def notify_send(headermsg, bodymsg=None, *, timeout_msec=5000):
    if not enable:
        return

    args = ['notify-send', '-t', str(timeout_msec), '--', str(headermsg)]
    if bodymsg is not None:
        args.append(htmlescape(str(bodymsg)))
    try:
        subprocess.run(args)
    except OSError:
        if warn:
            print('notify failed')
