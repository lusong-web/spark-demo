package com.eric.day01

import com.alibaba.fastjson.JSON
import com.eric.beans.Teacher
import com.eric.utils.SparkUtil
import org.apache.spark.rdd.RDD

object ToMysqlTest {
    def main(args: Array[String]): Unit = {
        val sc = SparkUtil.getSparkContext("test")
        val teacherRdd = sc.textFile("data/teacher")
        //通过 fastjson 解析数据
        val parseTeacher = teacherRdd.map(line => {
            //将每行数据解析为bean对象
            //解析可能不成功，解决异常
            try {
                val teacher = JSON.parseObject(line, classOf[Teacher])
                teacher
            } catch {
                case e: Exception => null
            }
        })
        //对解析的数据进行过滤
        val processedData: RDD[Teacher] = parseTeacher.filter(_ != null)

    }
}
