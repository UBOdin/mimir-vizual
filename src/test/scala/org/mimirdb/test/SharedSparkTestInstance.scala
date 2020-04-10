package org.mimirdb.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }

object SharedSparkTestInstance
{
  lazy val spark =
    SparkSession.builder
      .appName("Mimir-Vizual-Test")
      .master("local[*]")
      .getOrCreate()
}

trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark
}
