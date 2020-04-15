package org.mimirdb.vizual

import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Literal

import org.mimirdb.test.SharedSparkTestInstance

class VizualSpec
  extends Specification
  with SharedSparkTestInstance
{
  lazy val a = spark.range(20).toDF

  "Test Script" >> {
    val script = 
      Vizual.script
    /* 1 */ .insertColumn( "double", value = col("id") * 2 )
    /* 2 */ .deleteRows( Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9) )
    /* 3 */ .insertRow( values = Map("id" -> Literal(1l), "double" -> Literal(18l)))
    /* 4 */ .deleteColumn( "id" )
    /* 5 */ .renameColumn( "double", "theValue" )
    /* 6 */ .sort( "theValue", false )
    // /* 7 */ .update( "theValue", Some((0), col("theValue") * 2 )

    // print(script.get.mkString("\n"))

    val results = script(a).collect().toSeq

    // We start with 20 rows.  Line 2 halves it and line 3 adds one.
    results must haveSize(11)

    // print(results)

    // Sort on line 6 places the maximum element (38) at the front
    // Update on line 7 explicitly overrides with theValue*2=76
    //   ^- update skipped since we don't support positional addressing for now
    results(0).getAs[Long]("theValue") must be equalTo(38l)
    // results(0).getAs[Long]("theValue") must be equalTo(76l)

    // Line 2 trims the original range from 2-38 (every 2) to 20-38 (every 2)
    // Line 3 adds back an 18 (now 18-38 every 2)
    // Line 7 replaces the 38 (now 18-36 every 2 and also 76)
    // The following tests for 18-36 (every 2)
    results.map { _.getAs[Long]("theValue") } must contain(
      eachOf((9l until 19l).map { _ * 2 }:_*)
    )
  }

  // "Topo Sort with Updates" >> {
  //   val script = 
  //     Vizual.script 
  //           .insertColumn("const")
  //           .update("const", RowSelection.all, lit(200))
  //           .update("id", RowSelection.all, col("const") + 1)
  //           .update("const", RowSelection.all, lit(3))

  //   val results = script(a).collect().toSeq

  //   results must haveSize(20)

  //   results.map { _.getAs[Long]("id") }.toSet must be equalTo(Set(4l))
  // }
}