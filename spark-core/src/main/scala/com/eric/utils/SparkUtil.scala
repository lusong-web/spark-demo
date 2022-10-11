package com.eric.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {

    private val conf = new SparkConf()
    def getSparkContext(appName: String): SparkContext= {
        conf.setAppName(appName).setMaster("local[*]")
        new SparkContext(conf)
    }
}
