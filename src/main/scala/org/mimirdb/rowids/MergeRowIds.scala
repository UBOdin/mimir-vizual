package org.mimirdb.rowids

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._

object MergeRowIds
{
  def apply(exprs:Expression*): Expression = 
    exprs match {
      case Seq() => Literal(1l)
      case Seq(singleton) => Cast(singleton, LongType)
      case _ => Cast(new Murmur3Hash(exprs), LongType)
    }

  def apply(name: String, id: ExprId, exprs: Expression*): NamedExpression = 
    Alias(apply(exprs:_*), name)(id)

  // Following org.apache.spark.sql.catalyst.expressions.Murmur3Hash (in hash.scala)
  def literals(values:Long*): Long =
    values match {
      case Seq() => 1l
      case Seq(singleton) => singleton
      case _ => values.foldLeft(42){ (accum:Int, v:Long) => 
                                          Murmur3HashFunction.hash(v, LongType, accum).toInt }
                      .toLong
    }
}