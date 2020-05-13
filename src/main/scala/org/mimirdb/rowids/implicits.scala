package org.mimirdb.rowids

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object implicits 
{
  val ROWID_COLUMN    = col(AnnotateWithRowIds.ATTRIBUTE)
  val SEQUENCE_COLUMN = col(AnnotateWithSequenceNumber.ATTRIBUTE)

  implicit def wrapDataFrame(df: DataFrame) = new WrappedDataFrame(df)
}

class WrappedDataFrame(df: DataFrame)
{
  def withRowIds = AnnotateWithRowIds(df)
  def withSequenceNumber = AnnotateWithSequenceNumber(df)
}