package com.eric.day01

import com.eric.utils.SparkUtil
import org.apache.spark.rdd.RDD

object C02_Partitioner {
    def main(args: Array[String]): Unit = {
        val sc = SparkUtil.getSparkContext("partitioner2")
        val rdd = sc.textFile("data/cities.csv")
        val citiesRDD: RDD[(String, (String, String, String))] = rdd.map(line => {
            val city: Array[String] = line.split(",")
            (city(1), (city(0), city(2), city(3)))
        })
        val tuples = citiesRDD.distinct().collect()
        tuples.foreach(println)

    }
}
