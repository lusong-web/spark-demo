package com.eric.day01

import org.apache.spark.Partitioner

class MyPartition(cities: Array[String]) extends Partitioner{
    //设置分区个数
    override def numPartitions: Int = cities.size

    override def getPartition(key: Any): Int = {
        val city = key.toString
        cities.indexOf(city)
    }
}
