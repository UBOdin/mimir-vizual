package org.mimirdb.vizual.live

import org.specs2.mutable.Specification
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{
  Literal,
  BoundReference
}

class CellExpressionSpec
  extends Specification
{
  lazy val sheet = 
    BaseSheet.fromRows(Seq(
      Seq(1, 2, 3),
      Seq(4, 5, 6),
      Seq(7, 8, 9)
    ), Seq(
      StructField("A", IntegerType),
      StructField("B", IntegerType),
      StructField("C", IntegerType),
    ))

  "Evaluate Constant Expressions" >> {
    val e = CellExpression(Literal(2), Array(), sheet)
    e(0, 0, sheet) must be equalTo 2
  }

  "Evaluate Direct References" >> {
    val e = CellExpression(
      BoundReference(0, IntegerType, false),
      Array(("C", Left(0))),
      sheet
    )
    e(0, 0, sheet) must be equalTo 3
    e(1, 0, sheet) must be equalTo 6
    e(2, 0, sheet) must be equalTo 9
  }

  "Evaluate Offset References" >> {
    val e = CellExpression(
      BoundReference(0, IntegerType, false),
      Array(("C", Left(-1))),
      sheet
    )
    e(1, 0, sheet) must be equalTo 3
    e(2, 0, sheet) must be equalTo 6
  }

  "Evaluate References in a Different Reference Frame" >> {
    val e = CellExpression(
      BoundReference(0, IntegerType, false),
      Array(("C", Left(-1))),
      ReferenceFrame(Array(0l, 2l, 1l))
    )
    // Expression Reference Frame: (3, 9, 6)
    // Sheet Reference Frame:      (3, 6, 9)

    // Position 1 was originally position 2 (value=6), so 9 was in the slot before it
    e(1, 0, sheet) must be equalTo 9
    // Position 2 was originally position 1 (value=9), so 3 was in the slot before it
    e(2, 0, sheet) must be equalTo 3
  }

  "Evaluate Expressions That Need To Be Compiled" >> {
    {
      val e = CellExpression(lit(1), sheet)
      e(0, 0, sheet) must be equalTo 1
    }

    {
      val e = CellExpression(col("C"), sheet)
      e(0, 2, sheet) must be equalTo 3
    }

    {
      val e = CellExpression(col("[C:-1]"), sheet)
      e(1, 2, sheet) must be equalTo 3
    }

    {
      val e = CellExpression(col("[C:$1]"), sheet)
      e(1, 2, sheet) must be equalTo 6
    }

    {
      val e = CellExpression(col("[$C:1]"), sheet)
      e(1, 2, sheet) must be equalTo 9
    }

    {
      val e = CellExpression(col("[$C:$2]"), sheet)
      e(1, 2, sheet) must be equalTo 9
    }

    {
      val e = CellExpression(col("[$C:$2]"), sheet).withFrame(ReferenceFrame(Array(0l, 2l, 1l)))
      e(1, 2, sheet) must be equalTo 9
    }
  }

  "Evaluate Arithmetic" >> {
    {
      val e = CellExpression(col("[$C:$2]") + col("[A:$2]"), sheet)
      e(0, 0, sheet) must be equalTo 16
    }
  }
}
