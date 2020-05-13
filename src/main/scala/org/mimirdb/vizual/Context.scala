package org.mimirdb.vizual

import org.apache.spark.sql.{ Column, DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.mimirdb.rowids.implicits._
import org.mimirdb.util.DirectedGraph

object ContextTypes
{
  /**
   * The global identifier (see [[AnnotateWithRowIds]]) of a row
   */
  type RowIdentifier = Long
  /**
   * A global identifier for a column (the column name)
   */
  type ColumnIdentity = String

  /**
   * The position of a column (== schema.fieldNames.index(_:ColumnIdentity))
   */
  type ColumnIndex = Int
  /**
   * The position of a row in [[data]]
   */
  type RowIndex = Int
  /** 
   * An identifier for an update given its position in [[script]]
   */
  type UpdateReference = Long  
  /**
   * A reference to a block of cells
   */
  type BlockReference = Long

  type Region = (ColumnIdentity, Set[RowIdentifier])

  type Cell = (ColumnIdentity, RowIdentifier)
}

import ContextTypes._

case class Context(
  var data:scala.collection.mutable.Buffer[(RowIdentifier, scala.collection.mutable.Buffer[Any])], 
  var schema: StructType,
)
{
  var identifiers:Map[RowIdentifier, RowIndex] = 
    data.zipWithIndex
        .map { row => row._1._1 -> row._2 }
        .toMap


  val script = scala.collection.mutable.Buffer[Command]()
  val expressions = scala.collection.mutable.Buffer[(Update, Seq[Region])]()

  def apply(commands: Script)
  {
    for(command <- commands.get){ apply(command) }
  }

  private def without[T](arr:IndexedSeq[T], idx:Int): IndexedSeq[T] =
  {
    if(idx < 0){ arr }
    else if(idx == 0){ arr.slice(1, arr.size) }
    else if(idx >= arr.size){ arr }
    else if(idx == arr.size - 1){ arr.slice(0, idx) }
    else { arr.slice(0, idx) ++ arr.slice(idx+1, arr.size) }
  }

  def apply(command: Command)
  {
    command match {
      case DeleteColumn(column) => 
        {
          val idx = schema.fieldNames.indexOf(column)
          if(idx < 0){ throw new VizualException("Column does not exist", command) }
          schema = StructType(without(schema.fields, idx))
          for((_, row) <- data){ row.remove(idx) }
        }
      case DeleteRows(rows) => ???
      case InsertColumn(column, position) => ???
      case InsertRow(position, tag, identifier) => ???
      case MoveColumn(column, position) => ???
      case RenameColumn(column, newName) => ???
      case Sort(column, ascending) => ???
      case TagRowOrder(column, context) => ???
      case u@Update(column, rows, value) => 
        {
          val idx = schema.fieldNames.indexOf(column)
          value.expr match {
            case Literal(l, t) => {
              checkType(idx, t, command)
              update(idx, rows, l)
            }
            case _ => {
              val dependencies: Seq[Region] = Context.dependencies(rows, value)
              expressions.append( (u, dependencies) )
              checkType(idx, value.expr.dataType, command)
              updateEval(idx, rows, value)
            }
          }
        }
    }
    script += command
  }
  def checkType(column: ColumnIndex, t: DataType, cmd: Command)
  { 
    if(!schema.fields(column).dataType.equals(t)){
      throw new VizualException("Invalid data type", cmd) 
    }
  }
  def getRows(rows: RowSelection): Seq[RowIdentifier] = ???

  def update(column: ColumnIndex, rows: RowSelection, v: Any)
  { 
    for(row <- getRows(rows)){ update(column, row, v) } 
  }
  def update(column: ColumnIndex, row: RowIdentifier, v: Any)
  { 
    data(identifiers(row))._2(column) = v 
    ??? // need to trigger dependent updates as well
  }
  def updateEval(column: ColumnIndex, rows: RowSelection, expr: Column)
  { 
    for(row <- getRows(rows)){ updateEval(column, row, expr) } 
  }
  def updateEval(column: ColumnIndex, row: RowIdentifier, expr: Column)
  {
    ???
  }
}
object Context
{
  val IN_MEMORY_ROW_THRESHOLD = 1000*1000 // 1 million rows

  def apply(): Context = 
    Context(
      scala.collection.mutable.Buffer(1l -> scala.collection.mutable.Buffer[Any](null)), 
      StructType(Seq(StructField("unnamed", StringType)))
    )

  def apply(df:DataFrame): Context =
  {
    if(df.count() > IN_MEMORY_ROW_THRESHOLD){
      throw new RuntimeException("Spreadsheet context being initialized with too many rows.  Sample up to 1 million rows.")
    }
    val fieldCount = df.schema.fields.size
    Context(
      scala.collection.mutable.Buffer.concat(
        df.withRowIds
          .select((df.schema.fieldNames.map { df(_) } :+ ROWID_COLUMN):_*)
          .collect()
          .map { row => 
            (
              row.getLong(fieldCount),
              scala.collection.mutable.Buffer.concat(row.toSeq.take(fieldCount))
            )
          }
      ),
      df.schema
    )
  }

  def dependencies(rows: RowSelection, value: Column): Seq[Region] = 
  {
    ???
  }
}