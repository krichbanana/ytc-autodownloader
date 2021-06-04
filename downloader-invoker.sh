#!/usr/bin/env bash

# Apr 28, 2021

compress_lzip() {
    filename="${1?}"
    lzip -9 "${filename?}"
}

compress_zstd() {
    filename="${1?}"
    zstd -f -19 --rm "${filename?}"
}

compress() {
    compress_lzip "$1" || compress_zstd "$1"
}

run_chat_downloader() {
    outname="${1?}";
    url="${2?}";
    # No tee here
    chat_downloader -o "${outname?}".json --message_groups "all" --indent 2 -- "${url?}" > "${outname?}".stdout;
}

run_chat_downloader_waiting() {
    outname="${1?}";
    url="${2?}";
    run_chat_downloader "${outname?}" "${url}";
    until test -s "${outname?}.stdout"; do
        sleep 60;
        echo '(downloader)' "Retrying chat downloader for video ${url?}"
        run_chat_downloader "${outname?}" "${url}";
    done
    echo '(downloader)' "Finished downloading chat for video ${url?}"
    compress "${outname?}".json
    compress "${outname?}".stdout
    echo '(downloader)' "Finished compressing chat for video ${url?}"
}

if (( $# == 2 )); then
    cd chat-logs
    run_chat_downloader_waiting "${1?}" "${2?}"
else
    echo "usage: $0" '<outname>' '<url>'
fi
