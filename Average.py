#平均数：统计评价年龄
#	开发步骤分析
#	1：取出年龄：map
#	2:计算年龄综合：reduce
#	3.计算记录年龄总数： count
#	4.平均数



import sys
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: AvgAge<input>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    ageData = sc.textFile(sys.argv[1]).map(lambda x: x.split(" ")[1])
    totalAge = ageData.map(lambda x: int(x)).reduceByKey(lambda a, b: a+b)
    counts = ageData.count()
    avgAge = totalAge/counts

    print(counts)
    print(avgAge)
    print(ageData)

sc.stop()
