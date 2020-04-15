package org.mimirdb.vizual

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.catalyst.expressions.{ Literal }

import org.mimirdb.vizual.types._

case class Script(instructions: Seq[Command])
{
  private def add(cmd: Command*) = new Script(instructions ++ cmd)

  def get = instructions
  def apply(df: DataFrame) = Vizual(get, df)

  def deleteColumn(column: String)  = add(DeleteColumn(column))
  def deleteRows(rows: Set[RowIdentity]) 
                                    = add(DeleteRows(rows))
  def insertColumn(column: String, position: Integer = null, value: Any = null) 
                                    = value match {
                                        case col:Column => {
                                          add(
                                            InsertColumn(column, Option(position).map { _.toInt }, Literal(null)),
                                            Update(column, None, col)
                                          )
                                        }
                                        case _ => {
                                          add(InsertColumn(column, Option(position).map { _.toInt }, Literal(value)))
                                        }
                                      }
  def insertRow()                   = add(InsertRow(None, Map.empty))
  def insertRow(position: Long)     = add(InsertRow(Some(position), Map.empty))
  def insertRow(values: Map[String, Literal])
                                    = add(InsertRow(None, values.mapValues { Literal(_) }))
  def insertRow(position: Long, values: Map[String, Any])
                                    = add(InsertRow(Some(position), values.mapValues { Literal(_) }))
  def renameColumn(column: String, newName: String)
                                    = add(RenameColumn(column, newName))
  def sort(column: String, ascending: Boolean)
                                    = add(Sort(column, ascending))
  def update(column: String, value: String):Script 
                                    = update(column, expr(value), null)
  def update(column: String, value: Column):Script
                                    = update(column, value, null)
  def update(column: String, value: String, rows: Set[RowIdentity]):Script
                                    = update(column, expr(value), rows)
  def update(column: String, value: Column, rows: Set[RowIdentity]):Script
                                    = add(Update(column, Option(rows), value))

  override def toString = 
    instructions.zipWithIndex
                .map { cmd => s"${cmd._2}.\t ${cmd._1}" }
                .mkString("\n")
}