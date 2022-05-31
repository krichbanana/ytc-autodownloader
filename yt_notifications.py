#!/usr/bin/env python3
from yt_dlp.utils import traverse_obj
import json
from yt_dlp.utils import ExtractorError
from yt_dlp import YoutubeDL
from yt_dlp.extractor import YoutubeBaseInfoExtractor
import sys
import os
import time


def extract_notification_menu(response, continuation_list):
    notification_list = traverse_obj(response, ['actions', 0, 'openPopupAction', 'popup',
                                     'multiPageMenuRenderer', 'sections', 0, 'multiPageMenuNotificationSectionRenderer', 'items'], expected_type=list)
    if not notification_list:
        notification_list = traverse_obj(response, ['actions', 0, 'appendContinuationItemsAction', 'continuationItems'])
    for item in notification_list:
        for key in ['notificationRenderer', 'continuationItemRenderer']:
            value = item.get(key)
            if value:
                if key == 'notificationRenderer':
                    yield value
                    break
                elif key == 'continuationItemRenderer':
                    continuation_list[0] = value
                    return
    continuation_list[0] = None


def extract_notification(notification):
    # notificationRenderer
    video_id = traverse_obj(notification,
                            ('navigationEndpoint', 'watchEndpoint', 'videoId'),
                            ('navigationEndpoint', 'getCommentsFromInboxCommand', 'videoId'),
                            expected_type=str)
    if not video_id:
        return None
    thumbnails = traverse_obj(notification, ('videoThumbnail', 'thumbnails'), expected_type=list)
    status_text = traverse_obj(notification, ('shortMessage', 'simpleText'), expected_type=str)
    uploader = traverse_obj(notification, ('contextualMenu', 'menuRenderer', 'items', 1,
                            'menuServiceItemRenderer', 'text', 'runs', 1, 'text'), expected_type=str) or ''
    status_text = status_text.removeprefix(uploader + ' ')
    split_text = status_text.split(': ', 2)
    title = None
    action = None
    if len(split_text) == 2:
        action, title = split_text

    return {
        'video_id': video_id,
        'title': title,
        'thumbnails': thumbnails,
        'uploader': uploader,
        'action': action
    }


def extract_next_notification_continuation_data(renderer):
    # continuationItemRenderer
    ctoken = traverse_obj(renderer,
                          ('continuationEndpoint', 'getNotificationMenuEndpoint', 'ctoken'),
                          expected_type=str)
    if not ctoken:
        return
    return build_notification_continuation_query(ctoken)


def build_notification_continuation_query(ctoken):
    query = {
        'ctoken': ctoken
    }
    return query


def notification_menu_entries(ytie, headers, max_requests=5):
    continuation_list = [None]
    request_count = 0
    entries = []
    while request_count < max_requests:
        query = {}
        if continuation_list[0]:
            query = extract_next_notification_continuation_data(continuation_list[0])
        request_count += 1
        response = ytie._extract_response(ep='notification/get_notification_menu',
                                          headers=headers, check_get_keys=['responseContext', 'actions', 'trackingParams'],
                                          item_id='notification_menu', query=query)
        for notification in extract_notification_menu(response, continuation_list):
            entry = extract_notification(notification)
            if entry:
                entries.append(entry)
        if not continuation_list[0]:
            break

    return entries


def get_notifications(cookiefile, request_count=2, id_outfile='video_ids.txt'):
    params = {
        'cookiefile': cookiefile,
        'simulate': True,
        'verbose': False,
        'quiet': True
    }
    ytie = YoutubeBaseInfoExtractor()
    ytdl = YoutubeDL(params=params)
    ytie.set_downloader(downloader=ytdl)
    headers = ytie.generate_api_headers()
    cookiefile_out = cookiefile + ".out.txt"
    ytdl.cookiejar.save(filename=cookiefile_out)

    count = 0
    with open(id_outfile, "w") as fp:
        for entry in notification_menu_entries(ytie, headers, request_count):
            count += 1
            print(json.dumps(entry, ensure_ascii=False))
            print(entry['video_id'], file=fp)

    timenow = time.asctime()
    print(f'{timenow}: got {count} video entries', file=sys.stderr)


def main():
    cookiefile = sys.argv[1]
    max_requests = 2
    loop = False
    if len(sys.argv) > 2:
        max_requests = int(sys.argv[2])
    if len(sys.argv) > 3:
        loop = True
    print("cookiefile:", cookiefile, file=sys.stderr)
    print("max_requests:", max_requests, file=sys.stderr)
    os.chdir('oo')
    print("cwd:", os.getcwd(), file=sys.stderr)
    if loop:
        while True:
            get_notifications(cookiefile=cookiefile, request_count=max_requests)
            time.sleep(60)
    else:
        get_notifications(cookiefile=cookiefile, request_count=max_requests)


if __name__ == '__main__':
    main()
