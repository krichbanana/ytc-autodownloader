#!/usr/bin/env bash
pid="${1:?target pid not specified (bug)}"
delay=60
echo "watchdog: exit requested for pid $pid; timer set to kill any remaining process after $delay seconds"

atexit() {
    if kill -0 "$pid" 2>/dev/null; then
        echo "watchdog: warning: $pid still alive! Sending SIGTERM." >&2
        kill -TERM "$pid"
    fi
    sleep 5
    if kill -0 "$pid" 2>/dev/null; then
        echo "watchdog: warning: $pid still alive 5 seconds after SIGTERM! System stalled? Sending SIGKILL." >&2
        kill -KILL "$pid"
    fi
}
ctrlc() {
    echo 'watchdog: keyboard interrupt received, performing cleanup immediately.' >&2
}
trap "atexit" EXIT  # special bash handler called when program is exiting.
trap "ctrlc" INT    # handle ctrl-c

sleep "$delay"
echo "watchdog: timeout reached, performing any cleanup now."
