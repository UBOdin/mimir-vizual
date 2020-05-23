package org.mimirdb.vizual.live

import org.specs2.mutable.Specification

import org.apache.spark.sql.types.{ StructField, IntegerType }

class CellExpressionParserSpec
  extends Specification
{
  val sheet = BaseSheet(Seq(
                    StructField("A", IntegerType), 
                    StructField("B", IntegerType),
                    StructField("C", IntegerType)
                  ), rows = 3, default = 1)


  def eval(expr: String, row: Int = 1, col:Int = 1): Any =
    CellExpression(expr, sheet).apply(row, col, sheet)

  "Parse Literals" >> 
  {
    eval("1") must be equalTo(1)
    eval("'foo'").toString must be equalTo("foo")
  }

  "Parse Literal Expressions" >>
  {
    eval("1 * 2") must be equalTo(2)
    eval("pow(2, 3)") must be equalTo(8)
  }
}