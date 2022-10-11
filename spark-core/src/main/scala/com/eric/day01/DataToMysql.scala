package com.eric.day01

import com.alibaba.fastjson.JSON
import com.eric.beans.Teacher
import com.eric.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import java.sql.DriverManager

object DataToMysql {
    Logger.getLogger("org").setLevel(Level.ERROR)
    def main(args: Array[String]): Unit = {
        val sc = SparkUtil.getSparkContext("loadDataToMysql")
        val data = sc.textFile("data/teacher.dat")

        //对读取的数据进行解析处理
        val teachers: RDD[Teacher] = data.map(line => {
            try {
                val teacher = JSON.parseObject(line, classOf[Teacher])
                teacher
            } catch {
                case e: Exception => null
            }
        })
        //过滤掉脏数据
        val filteredTeachers = teachers.filter(_ != null)
        //将数据写入到 mysql
        //注册驱动
        //获取连接
        //对sql 进行预处理

        //对数据进行处理
        //Teacher(id:Int, name:String, age:Int, salary:Double, gender:String)
        filteredTeachers.foreachPartition(iter => {
            //获取连接
            val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "sun1998.")
            //获取预处理sql对象
            val pst = conn.prepareStatement("insert into  `tb_teachers` values (?, ?, ?, ?, ?)")
            iter.foreach(
                teacher => {
                    pst.setInt(1, teacher.id)
                    pst.setString(2, teacher.name)
                    pst.setInt(3, teacher.age)
                    pst.setDouble(4, teacher.salary)
                    pst.setString(5, teacher.gender)
                    pst.executeUpdate()
                }
            )
        })

    }
}
