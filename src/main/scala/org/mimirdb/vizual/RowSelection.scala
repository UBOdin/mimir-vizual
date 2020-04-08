package org.mimirdb.vizual

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions._

import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.rowids.AnnotateWithSequenceNumber

sealed trait RowSelection
{
  def predicate: Column
  def annotation: Option[Annotation]

  def annotateIfNeeded(df: DataFrame) =
  {
    annotation match { 
      case None => df
      case Some(annotate) => annotate(df)
    }
  }
}

case class ExplicitRowSelection(rows: Set[String]) extends RowSelection
{
  def predicate: Column = {
    if(rows.isEmpty) { return lit(false) }
    val rowid = col(AnnotateWithRowIds.ATTRIBUTE)
    val tests = rows.map { rowid === _ }
    tests.tail.fold(tests.head) { _ and _ }    
  }
  def annotation = Some(RowIdAnnotation())
}

case class PositionalRowSelection(low: Int, high: Option[Int]) extends RowSelection
{
  def predicate: Column = {
    if(low <= 0 && high.isEmpty) { return lit(true) }
    val seqAttribute = col(AnnotateWithSequenceNumber.ATTRIBUTE)
    val predicates = 
      Seq(
        if(low > 0) { Some(seqAttribute < low) }
        else { None },
        high.map { seqAttribute >= _ }
      ).flatten

    predicates.tail.fold(predicates.head) { _ or _ }
  }
  def annotation = Some(SequenceNumberAnnotation())
}

