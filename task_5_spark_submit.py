from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext
from __future__ import print_function

import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


ssc = StreamingContext(sc, 1)
topicPartion = TopicAndPartition('offers',0)
topic = 'offers'
fromOffset = {topicPartion: 0}

twitterKafkaStream = KafkaUtils.createDirectStream(ssc, [topic],{"bootstrap.servers": 'localhost:9092'}, fromOffsets=fromOffset)

tweets = twitterKafkaStream. \
        map(lambda (key, value): json.loads(value)). \
        map(lambda json_object: (json_object["text"]))

tweets.saveAsTextFiles('/tweets/')
tweets.pprint(10)


ssc.start()
ssc.awaitTermination()