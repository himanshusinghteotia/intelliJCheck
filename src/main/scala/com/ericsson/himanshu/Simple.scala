package com.ericsson.himanshu

import org.apache.spark.{SparkConf, SparkContext}

object Simple {
  def main(args: Array[String]) {
    println("Hello Scala & Spark")
    myMethod()
  }

  def myMethod() {
    val conf = new SparkConf().setAppName("Lines").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val linesDF = sc.textFile("C:\\Users\\eteohim\\Desktop\\First\\src\\Data\\myFile.txt").toDF("line")
    linesDF.show()
  }
}