# MongoDB 접속
mongo

# 데이터베이스 조회 및 선택
# show dbs
show databases
use twitter

# 컬렉션 조회
# show tables
show collections

# lang이 korea인 데이터 100만개 Insert
db.tweet_sample.find({"data.lang": 'ko'}).limit(1000000).forEach(function(p) {db.ko_tweet.insert(p);} );