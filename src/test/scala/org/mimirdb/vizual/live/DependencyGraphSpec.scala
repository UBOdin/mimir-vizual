package org.mimirdb.vizual.live

import org.specs2.mutable.Specification

import org.apache.spark.sql.types.{ StructField, IntegerType }

class DependencyGraphSpec
  extends Specification
{
  type AddNodeFn = ((String, Int, Int, String) => Unit)

  val emptySheet = BaseSheet(Seq(
                      StructField("A", IntegerType), 
                      StructField("B", IntegerType),
                      StructField("C", IntegerType)
                    ), rows = 3, default = 1)

  def withGraph[A](op: (AddNodeFn, DependencyGraph) => A) =
  {
    var graph = new DependencyGraph()
    op( 
      {
        (col:String, low:Int, high:Int, expr:String) => 
          graph.add(
            CellExpression(expr, emptySheet), 
            Box(col, (low.toLong until high.toLong).toSet)
          )
      },
      graph
    )
  }

  "Support Literals" >> {
    ok
  }
}