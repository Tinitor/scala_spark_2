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
      option("inferschema",true).
      csv("data/bank.csv")

    input.printSchema
    input.show(50, false)

    //Escribimos en parquet con particiones por campo "marital"
    val parquet_file = input.write.mode("overwrite").partitionBy("marital").parquet("data/parquet/status.parquet")

    //seleccionamos la media del saldo disponible dependiendo de su estado "marital"
    val agrupacion = input.groupBy("marital").avg("balance")
    agrupacion.show


    spark.stop()
  }

}
