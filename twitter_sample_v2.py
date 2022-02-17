import datetime
import json
import requests
import os
import tqdm
import pymongo
from config.config import (
    BEARER_TOKEN,
)  # Twitter TOKEN 정보 불러오기 (발급 필요, 링크: https://developer.twitter.com/)

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
# bearer_token = os.environ.get("BEARER_TOKEN")
# BEARER_TOKEN = ""
KST = datetime.timezone(datetime.timedelta(hours=9))

"""
    Twitter API v2 calls 예시
    링크: https://developer.twitter.com/apitools/api?endpoint=%2F2%2Ftweets%2Fsample%2Fstream&method=get
"""
# get_url = "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=id,created_at,text,author_id,in_reply_to_user_id,referenced_tweets,attachments,withheld,geo,entities,public_metrics,possibly_sensitive,source,lang,context_annotations,non_public_metrics,promoted_metrics,organic_metrics,conversation_id,reply_settings&user.fields=id,name,username,location&place.fields=id,name,country_code,place_type,country,geo"
get_url = "https://api.twitter.com/2/tweets/sample/stream"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r


# url = "https://api.twitter.com/2/tweets/sample/stream"
response = requests.request("GET", get_url, auth=bearer_oauth, stream=True)

mongo = pymongo.MongoClient()

for line in tqdm.tqdm(response.iter_lines(), mininterval=30, maxinterval=30):
    if line:
        tweet = json.loads(line)
        tweet["_timestamp"] = datetime.datetime.now(tz=KST).isoformat()  # timestamp 추가
        mongo.twitter.tweet_all_sample.insert_one(tweet)  # 저장
