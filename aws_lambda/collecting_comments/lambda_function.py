import re
import hashlib
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient

request = urllib.request.Request('http://live.afreecatv.com:8057/api/get_broadlist_main_info.php')
html = urllib.request.urlopen(request).read()
html = html.decode()

html = re.sub('\\\\/', '/', html)

match = re.search('"HOTISSUE_VOD_DATA":(\[.*\],"STAFF_PICKS_RESULT")', html)
title_numbers = re.findall('"title_no":"(.*?)"', match.group(1))
user_ids = re.findall('"user_id":"(.*?)"', match.group(1))

mongo_client = MongoClient('localhost')
db = mongo_client.afreecatv_glance
collection = db.comments

for (title_number, user_id) in [(title_numbers[i], user_ids[i]) for i in range(0, len(title_numbers))]:
    last_number = '0'

    while 1:
        request = urllib.request.Request('http://stbbs.afreecatv.com/api/bbs_memo_action.php?nTitleNo=' + title_number + '&bj_id=' + user_id + '&nPageNo=1&szAction=get&nVod=1&nLastNo=' + last_number)
        data = urllib.request.urlopen(request).read()
        data = data.decode('unicode_escape')

        data = re.sub('\n', ' ', data)
        data = re.sub('<br.*?>', ' ', data)
        data = re.sub('<img.*?>', '', data)

        match = re.search('"list_data":(\[.+\])},"TOTAL_CNT"', data)

        if match is None:
            break

        comment_numbers = re.findall('"p_comment_no":"(.*?)"', match.group(1))
        user_ids_2 = re.findall('"user_id":"(.*?)"', match.group(1))
        comments = re.findall('"comment":"(.*?)"', match.group(1))
        registration_dates = re.findall('"reg_date":"(.*?)"', match.group(1))

        last_number = comment_numbers[-1]

        for (user_id_2, comment, registration_date) in [(user_ids_2[i], comments[i], registration_dates[i]) for i in range(0, len(user_ids_2))]:
            registration_date = datetime(int(registration_date[0:4]), int(registration_date[5:7]), int(registration_date[8:10]), int(registration_date[11:13]), int(registration_date[14:16]))
            
            sha256 = hashlib.sha256()
            sha256.update(bytes(title_number + ';' + user_id_2 + ';' + comment + ';' + str(registration_date), 'utf-8'))
            hash_digest = sha256.hexdigest()

            doc = collection.find_one({ 'hash_digest': hash_digest })

            if doc is not None:
                continue

            collection.insert_one({
                'title_number': title_number,
                'user_id': user_id_2,
                'comment': comment,
                'registration_date': registration_date,
                'hash_digest': hash_digest,
                'date': datetime.utcnow()
            })
