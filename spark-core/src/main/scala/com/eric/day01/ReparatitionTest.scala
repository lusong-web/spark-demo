package com.eric.day01

import com.eric.utils.SparkUtil

object RepartitionTest {
    def main(args: Array[String]): Unit = {
        val sc = SparkUtil.getSparkContext("repartition")
        val rdd = sc.textFile("data/test.txt", 2)
        println(rdd.getNumPartitions)
        //改变分区
        val rdd2 = rdd.repartition(3)
        println(rdd2.getNumPartitions)
    }
}
