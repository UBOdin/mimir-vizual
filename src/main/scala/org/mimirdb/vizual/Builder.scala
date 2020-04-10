package org.mimirdb.vizual

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.catalyst.expressions.SortDirection


class Builder(instructions: List[Command])
{
  private def add(cmd: Command) = new Builder(cmd :: instructions)

  def get = instructions.reverse.toSeq
  def apply(df: DataFrame) = Vizual(get, df)

  def deleteColumn(column: String)  = add(DeleteColumn(column))
  def deleteRow(rows: RowSelection) = add(DeleteRow(rows))
  def insertColumn(column: String, position: Integer = null, values: Column = null) 
                                    = add(InsertColumn(column, Option(position).map { _.toInt }, Option(values)))
  def insertRow(position: Integer = null, values: Seq[Column] = null)
                                    = add(InsertRow(Option(position).map { _.toInt } , Option(values)))
  def renameColumn(column: String, newName: String)
                                    = add(RenameColumn(column, newName))
  def sort(column: String, ascending: SortDirection)
                                    = add(Sort(column, ascending))
  def update(column: String, rows: RowSelection, value: Column)
                                    = add(Update(column, rows, value))

  override def toString = instructions.reverse.map { _.toString }.mkString("\n")
}

