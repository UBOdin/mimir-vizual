package org.mimirdb.vizual

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions._

import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.rowids.AnnotateWithSequenceNumber

sealed trait RowSelection
{
  def predicate: Column
  def annotation: Option[Annotation]

  /**
   * Best-effort test of intersection with false positives
   * 
   * return false if no intersection guaranteed.
   * return true if intersection possible
   */
  def canIntersect(other: RowSelection): Boolean

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
  def canIntersect(other: RowSelection): Boolean = {
    other match { 
      case ExplicitRowSelection(otherRows) => !(rows & otherRows).isEmpty
      case _ => true
    }
  }
  override def toString: String = s"RowsIDs[${rows.mkString(", ")}"
}

case class PositionalRowSelection(low: Long, high: Option[Long]) extends RowSelection
{
  def predicate: Column = {
    if(low <= 0 && high.isEmpty) { return lit(true) }
    val seqAttribute = col(AnnotateWithSequenceNumber.ATTRIBUTE)
    val predicates = 
      Seq(
        if(low > 0) { Some(seqAttribute >= low) } else { None },
        high.map { seqAttribute < _ }
      ).flatten

    predicates.tail.fold(predicates.head) { _ or _ }
  }
  def annotation = Some(SequenceNumberAnnotation())
  def canIntersect(other: RowSelection): Boolean = {
    other match { 
      case PositionalRowSelection(otherLow, otherHigh) => {
        if(!high.isEmpty && otherLow > high.get){ return false }
        if(!otherHigh.isEmpty && low > otherHigh.get){ return false }
        return true
      }
      case _ => true
    }
  }
  override def toString: String = 
    if(low < 0 && high.isEmpty) { "All Rows" }
    else {
      "Rows["+Seq[Option[String]](
        if(low > 0) { Some(s"From $low") } else { None },
        high.map { h => s"To $h" }
      ).flatten.mkString(" ")+"]"
    }
}
object PositionalRowSelection{
  def apply(low: Long, high: Long):PositionalRowSelection = 
    PositionalRowSelection(low, Some(high))
  def from(low: Long) = PositionalRowSelection(low, None)
  def to(high: Long) = PositionalRowSelection(0, Some(high))
  def at(pos: Long) = PositionalRowSelection(pos, Some(pos+1))
  def all = PositionalRowSelection(0, None)
}
