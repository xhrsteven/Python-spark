from pyspark import SparkConf, SparkContext

if __name__ == ' __main__ ':

    conf =SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)

#map: map(func) 将func函数作用到数据集的每一个元素上，生产一个新的分布式的数据集返回;
    def my_map():
        data = [1, 2, 3, 4, 5]
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x: x*2)
        print(rdd2.collect())

    def my_map2():
        a = sc.parallelize(["dog","tiger","lion","cat","panther"])
        b = a.map(lambda x: (x, 1))
        print(b.collect())

#filter: filter(func)选出所有function返回值为true的元素，生产一个新的分布式的数据集返回
    def my_fliter():
        data = [1, 2, 3, 4, 5]
        rdd1 = sc.parallelize(data)
        mapRdd = rdd1.map(lambda x: x*2)
        filterRdd = mapRdd.filter(lambda x: x>5)
        print(filterRdd.collect())

#flatMap: flatMap(func)输入的item能够被map到0或者多个items输出，返回值是一个Sequence
    def my_flatMap():
        data = ["Hello Spark", "Hello World", "Hello World"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")))

#groupByKey: 把相同的key的数据分发到一起
#[{'Hello': [1, 1, 1]}, {'Spark': [1]}, {'World': [1, 1]}]
    def my_groupBy():
        data = ["Hello Spark", "Hello World", "Hello World"]
        rdd =sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x:(x,1))
        groupByRdd =mapRdd.groupByKey()
        print(groupByRdd.collect())
        print(groupByRdd.map(lambda x:{x[0]:list(x[1])}).collect())
        
#reduceByKey
#	reduceByKeyRDD = mapRdd.reduceByKey(lambda a,b : a+b)把相同的key的数据分发到一起并进行相应的计算
#[('Hello', 3), ('Spark', 1), ('World', 2)] 

    def my_reduceByKey():
        data = ["Hello Spark", "Hello World", "Hello World"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        reduceByKeyRdd = mapRdd.reduceByKey(lambda a,b : a+b)
        
#需求： 按wordcount结果中出现的次数降序排列--wordcount
    def my_sort():
        data = ["Hello Spark", "Hello World", "Hello World"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        reduceByKeyRdd = mapRdd.reduceByKey(lambda a, b: a + b)
        #字母的降序排列
        reduceByKeyRdd.sortByKey(False).collect()
        #数字降序排列->字母的降序排列
        reduceByKeyRdd.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0])).collect()

#取全部element
    def my_union():
        a = sc.parallelize([1, 2, 3, 4])
        b = sc.parallelize([2, 3, 4, 5])
        a.union(b).collect()

#取共有element
    def my_distinct():
        a = sc.parallelize([1, 2, 3, 4])
        b = sc.parallelize([2, 3, 4, 5])
        a.union(b).distinct().collect()
        
#Join共有element
    def my_join():
        a1 = sc.parallelize([("A","a1"),("C","c1"),("D","d1"),("F","f1"),("F","f2")])
        b1 = sc.parallelize([("A","a2"),("C","c2"),("C","c3"),("E","e1")])
        a1.join(b1).collect()


        sc.stop()
