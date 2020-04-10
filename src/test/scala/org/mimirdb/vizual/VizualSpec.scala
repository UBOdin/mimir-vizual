package org.mimirdb.vizual

import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Descending

import org.mimirdb.test.SharedSparkTestInstance

class VizualSpec
  extends Specification
  with SharedSparkTestInstance
{
  lazy val a = spark.range(20).toDF

  "Test Script" >> {
    val script = 
      Vizual.script
    /* 1 */ .insertColumn( "double", values = col("id") * 2 )
    /* 2 */ .deleteRow( PositionalRowSelection(0, Some(10)) )
    /* 3 */ .insertRow( values = Seq(lit(1l), lit(18l)))
    /* 4 */ .deleteColumn( "id" )
    /* 5 */ .renameColumn( "double", "theValue" )
    /* 6 */ .sort( "theValue", Descending )
    /* 7 */ .update( "theValue", PositionalRowSelection(0, Some(1)), col("theValue") * 2 )

    val results = script(a).collect().toSeq

    // We start with 20 rows.  Line 2 halves it and line 3 adds one.
    results must haveSize(11)

    // Sort on line 6 places the maximum element (38) at the front
    // Update on line 7 explicitly overrides with theValue*2=76
    results(0).getAs[Long]("theValue") must be equalTo(76l)

    // Line 2 trims the original range from 2-38 (every 2) to 20-38 (every 2)
    // Line 3 adds back an 18 (now 18-38 every 2)
    // Line 7 replaces the 38 (now 18-36 every 2 and also 76)
    // The following tests for 18-36 (every 2)
    results.map { _.getAs[Long]("theValue") } must contain(
      eachOf((9l until 19l).map { _ * 2 }:_*)
    )
  }
}