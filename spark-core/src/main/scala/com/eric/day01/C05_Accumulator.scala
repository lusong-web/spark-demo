package com.eric.day01

import com.alibaba.fastjson.JSON
import com.eric.beans.Teacher
import com.eric.utils.SparkUtil
import org.apache.spark.util.LongAccumulator

object C05_Accumulator {
    def main(args: Array[String]): Unit = {
        val cs = SparkUtil.getSparkContext("closure")
        val rdd = cs.textFile("data/teacher.dat")

        val accumulator = new LongAccumulator
        //register a accumulator
        cs.register(accumulator)
        val rdd2 = rdd.map(line => {
            try {
                JSON.parseObject(line, classOf[Teacher])
            } catch {
                case e: Exception => accumulator.add(1)
            }
        })
        rdd2.collect()
        // print accumulator's value
        println(accumulator.value)
    }
}
