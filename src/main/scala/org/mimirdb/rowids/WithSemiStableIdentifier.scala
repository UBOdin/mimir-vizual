package org.mimirdb.rowids

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._

object WithSemiStableIdentifier
{

  def apply(plan: LogicalPlan, attribute: Attribute, session: SparkSession, offset:Long = 1): LogicalPlan =
  {
    val countAttribute = AttributeReference(AnnotateWithSequenceNumber.ATTRIBUTE, LongType, false)()
    val planWithSequenceNumber = 
      AnnotateWithSequenceNumber(plan, session, attribute = countAttribute, offset = offset)

    Project(
      plan.output :+ Alias(
        MergeRowIds((countAttribute +: plan.output):_*),
        attribute.name
      )(attribute.exprId),
      planWithSequenceNumber
    )
  }
}