package org.mimirdb.vizual.live

import scala.collection.mutable.{ Buffer, Map }

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ StructField, StructType }

/**
 * An encapsulated spreadsheet
 * @param data The spreadsheet's data buffer, annotated with RowIds
 */
class BaseSheet(
  data: Buffer[(Seq[Any], Long)],
  fields: Seq[StructField]
) extends ReferenceFrame
{ 
  /**
   * Capture a snapshot of the sort order of the table
   */
  def order = data.map { _._2 }.toArray
  
  /**
   * Retrieve the schema of the dataframe represented by this sheet
   */
  def schema = StructType(fields)
  
  /**
   * Retrieve the rowid for the specified position
   */
  def rowid(pos: Int): Long = data(pos)._2
  
  /**
   * Retrieve the position for a specified reference frame
   */
  def position(rowid: Long): Int = positions(rowid)

  /**
   * Retain a snapshot of the currently active reference fame
   */
  def snapshotReferenceFrame = ReferenceFrame(order)

  /**
   * Retrieve the type of the column at the specified position
   */
  def dataType(column: Int) = fields(column).dataType

  /**
   * Retrieve the type of the column with the specified name
   */
  def dataType(column: String) = fields(columnIndex(column)).dataType

  /**
   * A lookup table mapping columns to positions in the schema
   */
  val columnIndex: Map[String, Int] = 
    Map( fields.zipWithIndex
               .map { col => col._1.name -> col._2 }:_*)
  /**
   * A lookup table mapping rowids to positions in [[data]]
   */
  val positions: Map[Long, Int] = 
    Map( data.zipWithIndex
             .map { row => row._1._2 -> row._2 }:_*)


  /**
   * Retrieve the contents of a specific cell
   * @param row  The row to retrieve (0-based)
   * @param col  The column to retrieve (0-based)
   */
  def apply(row: Int, col: Int) = 
  {
    data(row)._1(col)
  }

  /**
   * Modify the contents of a specific cell
   */
  def update(row: Int, col: Int)(value: Any)
  {
    val (contents, rowid) = data(row)
    data(row) = (
      contents.updated(col, value), 
      rowid
    )
  }

  /**
   * Modify the contents of a specific cell by evaluating a provided expression
   */
  def eval(row: Int, col: Int)(cell: CellExpression)
  {
    update(row, col) { cell(row, col, this) }
  }
}

object BaseSheet
{
  def apply(schema: StructType): BaseSheet = apply(schema.fields)
  def apply(schema: StructType, rows: Int): BaseSheet = apply(schema.fields, rows)
  def apply(fields: Seq[StructField]): BaseSheet = apply(fields, 1)
  def apply(fields: Seq[StructField], rows: Int): BaseSheet = 
  {
    new BaseSheet(
      Buffer.range(0, rows).map { id => (fields.map { _ => null:Any }, id.toLong) },
      fields
    )
  }
  def fromRows(value: Seq[Seq[Any]], fields: Seq[StructField]) = 
    new BaseSheet(
      Buffer(value.zipWithIndex.map { r => (r._1, r._2.toLong) }:_*),
      fields
    )
}