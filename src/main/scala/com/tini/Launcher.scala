package com.tini

import org.apache.spark.sql.SparkSession

object Launcher {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SomeAppName")
      .config("spark.master", "local")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")

    // Lectura archivo parquet
    val input = spark.read.
      option("delimitator", ",").
      option("header", true).
      csv("data/bank.csv")

    input.printSchema
    input.show(50, false)

    spark.stop()
  }

}
