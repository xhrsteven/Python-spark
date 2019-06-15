#TopN 案例
 #	1）input ： 1/n 文件 文件夹 后缀名
	#2）求某个维度topn
	#3) 开发步骤分析
		#文本内容的每一行根据需求提前出你所需要的字段:map
		#单词 ==>（单词，1）：map
		#把所有相同单词的计数相加得到最终的结果：reduceByKey
		#取最多出现次数的降序： sortByKey

import sys
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: topn<input>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    counts = sc.textFile(sys.argv[1]) \
        .map(lambda x: x.split("\t")) \
        .map(lambda x: (x[1], 1)) \
        .reduceByKey(lambda a,b: a+b) \
        .map(lambda x: (x[1],x[0])) \
        .sortByKey(False) \
        .map(lambda x : (x[1],x[0])).take(3)

    for (word, counts) in counts:
        print("%s : %i" %(word, count))

    sc.stop()
