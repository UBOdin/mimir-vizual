package org.mimirdb.vizual

import org.apache.spark.sql.DataFrame

import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.rowids.AnnotateWithSequenceNumber

sealed trait Annotation
{
  def apply(df: DataFrame): DataFrame
}

case class RowIdAnnotation() extends Annotation
{
  def apply(df: DataFrame) = AnnotateWithRowIds(df)
}
case class SequenceNumberAnnotation() extends Annotation
{
  def apply(df: DataFrame) = AnnotateWithSequenceNumber(df)
}