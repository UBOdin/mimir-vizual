package org.mimirdb.rowids

import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.mimirdb.test.SharedSparkTestInstance

class AnnotateWithRowIdsSpec
  extends Specification
  with SharedSparkTestInstance
{
  lazy val a = 
    spark.createDataFrame( ((0 until 50).map { x => (x, -x) }.toSeq) )
  lazy val b = 
    spark.createDataFrame( ((0 until 51).map { x => (x, -x) }.toSeq) )
  
  def getRowIds(df: DataFrame): Set[Long] =
  { 
    AnnotateWithRowIds(df)
        .collect()
        .map { _.getAs[Long](AnnotateWithRowIds.ATTRIBUTE) }
        .toSet
  }

  "Base Relations" >> {
    val baseRowIds = getRowIds(a)
    baseRowIds must haveSize(50)
    (getRowIds(b) -- baseRowIds) must haveSize(1)

    val reversed = getRowIds(a.orderBy("_2"))
    reversed must haveSize(50)
    reversed must contain(eachOf(baseRowIds))
  }

  "Project+Filter" >> {
    getRowIds(
      a.filter { col("_1") < 10 }
       .select( col("_2") )
    ) must haveSize(10)
  }

  "Union" >> {
    getRowIds(a.union(b)) must haveSize(101)
  }

  "Join" >> {
    getRowIds(a.join(b, a("_1") === b("_1"))) must haveSize(50)
  }

  "Range" >> {
    getRowIds(spark.range(50).toDF) must haveSize(50)
  }

  "Aggregate" >> {
    getRowIds(
      a.agg(sum(a("_1")))
    ) must haveSize(1)

    getRowIds(
      a.select { (a("_1") / 10).cast("int").as("div") }
       .groupBy("div")
       .count()
    ) must haveSize(5)

  }
}