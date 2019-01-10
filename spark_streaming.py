# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

# Stream으로 들어온 값에 _hehe를 붙여서 출력하는 함수입니다.
def just_print(line):
    return line +"_hehe"

# Spark Streaming을 위한 Context를 생성하는데, local[2]는 2개의 로컬 쓰레드를 생성한다는 것입니다. 
sc = SparkContext("local[2]", "SparkStreamingTest")
# 앞서 만든 Spark Context를 이용하여 StreamingContext가 1초마다 batch작업을 하도록 생성합니다.
ssc = StreamingContext(sc, 1)

# TCP socket stream으로 구성된 Discretized Stream을 생성합니다. sys.argv[1], [2] 에는 host, port가 들어갑니다.
# 결과적으로 host:port로 socket연결을 하고 스트림을 생성하게 됩니다. (먼저 포트가 열려 있어야겠죠!?)
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
recv_data = lines.map(just_print)
recv_data.pprint()

# 만들어진 Stream(socket base)을 시작합니다.
ssc.start()

# 앞서 시작한 Stream 연산(작업)이 끝날때 까지 기다립니다.
ssc.awaitTermination()

