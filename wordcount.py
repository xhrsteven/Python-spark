import sys
# import os
#
# os.environ['JAVA_HOME'] = 'D:\Program Files\Java\jdk1.8.0_144'
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: wordcount<input> ", file=sys.stderr)
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
    printresult()

    # def saveFile():
    #     sc.textFile(sys.argv[1]) \
    #         .flatMap(lambda line: line.split("\t")) \
    #         .map(lambda x: (x, 1)) \
    #         .reduceByKey(lambda a, b: a + b) \
    #         .saveAsFile(sys.argv[2])
    #
    # saveFile()

    sc.stop()