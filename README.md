# spark-twitter
Spark example

# Spark 연동
- 참고자료: https://urame.tistory.com/entry/MongoDB-Spark-Connection-%ED%85%8C%EC%8A%A4%ED%8A%B8
```
pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

df = (spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost/twitter.tweet_sample").load())
df.createOrReplaceTempView("tweets")

query = '''
select lang, count(*) as count
from tweets where delete is null group by lang order by 2 desc
'''

spark.sql(query).show(3)
spark.sql("select data from tweets").show(3)
spark.sql("select data.lang from tweets").show(3)
spark.sql("select data.lang, count(*) as cnt from tweets group by data.lang order by data.lang").show(20) # 컴퓨터 터질듯..

>>> spark.sql("select data.lang, count(*) as cnt from tweets group by data.lang order by data.lang").show(20)
[Stage 4:==================================================>    (263 + 4) / 285]

>>> spark.sql("select data.lang, count(*) as cnt from tweets group by data.lang order by data.lang").show(20)
+----+-------+                                                                  
|lang|    cnt|
+----+-------+
|null|      4|
|  am|   1551|
|  ar| 621032|
|  bg|   1100|
|  bn|  20992|
|  bo|      9|
|  ca|  22761|
| ckb|    473|
|  cs|  10410|
|  cy|   7741|
|  da|   9350|
|  de|  80355|
|  dv|    288|
|  el|  18146|
|  en|4899459|
|  es|1024125|
|  et|  33288|
|  eu|   9281|
|  fa|  80936|
|  fi|  12874|
+----+-------+

>>> spark.sql("select data.lang, count(*) as cnt from tweets group by data.lang order by cnt").show(20)
[Stage 7:=======>                                                (40 + 4) / 286]

```