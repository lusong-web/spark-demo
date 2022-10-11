package com.eric.day01

import com.eric.utils.SparkUtil

object C01_Partitioner {
    def main(args: Array[String]): Unit = {
        val sc = SparkUtil.getSparkContext("partitioner")
        //分区
        val rdd = sc.makeRDD(List(1, 2, 4, 6, 9), 2)
        val rdd1 = rdd.map(_ * 2)
        val rdd2 = rdd.map(_ * 10)

        val kvRDD = rdd1.map(e => (e, e * 10))
        val rdd3 = kvRDD.groupByKey()
        rdd3.foreach(println)

        /*
        The default partitioner is RangePartitioner
        The underlining call to sortBy is sortByKey
         */
        rdd1.keyBy(e => e).sortByKey()
    }
}
