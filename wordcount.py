#词频统计多角度迭代
import sys
# import os
# os.environ['JAVA_HOME'] = 'D:\Program Files\Java\jdk1.8.0_144'
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: wordcount<input> <output>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc= SparkContext(conf=conf)


    def printresult():
        counts = sc.textFile(sys.argv[1]) \
            .flatMap(lambda line: line.split("\t")) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b)

        output = counts.collect()
        for (word, count) in output:
            print("%s ： %i" % (word, count))

    def saveFile():
        sc.textFile(sys.argv[1]) \
            .flatMap(lambda line: line.split("\t")) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .saveAsTextFile(sys.argv[2])

    saveFile()

    sc.stop()

#./spark-submit --master local[2] --name spark0301 /opt/module/spark-2.4.3-bin-hadoop2.7/examples/spark0301.py file:///opt/module/spark-2.4.3-bin-hadoop2.7/data/wc file:///opt/module/spark-2.4.3-bin-hadoop2.7/data/tmp
