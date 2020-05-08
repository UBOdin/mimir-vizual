package org.mimirdb.vizual

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.functions.{ expr, lit }
import org.apache.spark.sql.catalyst.expressions.{ Literal }
import org.apache.spark.sql.types.StructType

import org.mimirdb.vizual.types._

case class Script(instructions: Seq[Command])
{
  private def add(cmd: Command*) = new Script(instructions ++ cmd)

  def get = instructions
  def compiled(schema: StructType) = Normalize(instructions, schema)
  def apply(df: DataFrame) = compiled(df.schema)(df)

  def deleteColumn(column: String)  = add(DeleteColumn(column))
  def deleteRows(rows: RowSelection) 
                                    = add(DeleteRows(rows))
  def insertColumn(column: String, position: Integer = null, value: Any = null) 
                                    = add(
                                        InsertColumn(column, Option(position).map { _.toInt }),
                                        Update(column, AllRows(), 
                                          value match {
                                            case col:Column => col
                                            case _ => lit(value)
                                          })
                                      )
  def insertRow()                   = add(InsertRow(None, None))
  def insertRow(position: Long)     = add(InsertRow(Some(position), None))
  def renameColumn(column: String, newName: String)
                                    = add(RenameColumn(column, newName))
  def sort(column: String, ascending: Boolean)
                                    = add(Sort(column, ascending))
  def update(column: String, value: String):Script 
                                    = update(column, expr(value), AllRows())
  def update(column: String, value: Column):Script
                                    = update(column, value, AllRows())
  def update(column: String, value: String, rows: RowSelection):Script
                                    = update(column, expr(value), rows)
  def update(column: String, value: Column, rows: RowSelection):Script
                                    = add(Update(column, rows, value))
  def update(values: Map[String, Column]):Script
                                    = update(values, AllRows())
  def update(values: Map[String, Column], rows: RowSelection):Script
                                    = add(values.map { v => Update(v._1, rows, v._2) }.toSeq:_*)

  override def toString = 
    instructions.zipWithIndex
                .map { cmd => s"${cmd._2}.\t ${cmd._1}" }
                .mkString("\n")
}