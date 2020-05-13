package org.mimirdb.vizual.live

import org.specs2.mutable.Specification
import org.apache.spark.sql.types._

class CellExpressionSpec
  extends Specification
{
  lazy val sheet = 
    Sheet.fromRows(Seq(
      Seq(1, 2, 3),
      Seq(4, 5, 6),
      Seq(7, 8, 9)
    ), Seq(
      StructField("A", IntegerType),
      StructField("B", IntegerType),
      StructField("C", IntegerType),
    ))

  "Evaluate Constant Expressions" >> {
    CellExpression()
  }
}
