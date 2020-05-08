package org.mimirdb.vizual

import play.api.libs.json._

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.{ Expression, Attribute }

import org.mimirdb.rowids._
import org.mimirdb.vizual.types._

sealed trait Command
sealed trait PreprocessCommand extends Command
sealed trait PostprocessCommand extends Command

object Command
{ 
  implicit val format = Format[Command](
    new Reads[Command] { def reads(j: JsValue) = 
      j.as[JsObject].value("type").as[String] match {
        case "DeleteColumn" => JsSuccess(j.as[DeleteColumn])
        case "DeleteRows"   => JsSuccess(j.as[DeleteRows])
        case "InsertColumn" => JsSuccess(j.as[InsertColumn])
        case "InsertRow"    => JsSuccess(j.as[InsertRow])
        case "MoveColumn"   => JsSuccess(j.as[MoveColumn])
        case "RenameColumn" => JsSuccess(j.as[RenameColumn])
        case "Sort"         => JsSuccess(j.as[Sort])
        case "Update"       => JsSuccess(j.as[Update])
        case "TagRowOrder"  => JsSuccess(j.as[TagRowOrder])
        case _ => JsError()
      }
    },
    new Writes[Command] { def writes(c: Command) =
      { 
        val (t: String, j:JsValue) = 
          c match {
            case j:DeleteColumn => "DeleteColumn" -> Json.toJson(j)
            case j:DeleteRows   => "DeleteRows"   -> Json.toJson(j)
            case j:InsertColumn => "InsertColumn" -> Json.toJson(j)
            case j:InsertRow    => "InsertRow"    -> Json.toJson(j)
            case j:MoveColumn   => "MoveColumn"   -> Json.toJson(j)
            case j:RenameColumn => "RenameColumn" -> Json.toJson(j)
            case j:Sort         => "Sort"         -> Json.toJson(j)
            case j:Update       => "Update"       -> Json.toJson(j)
            case j:TagRowOrder  => "TagRowOrder"  -> Json.toJson(j)
          }
        JsObject(
          j.as[Map[String,JsValue]] + ("type" -> JsString(t))
        )
      }
    }
  )
}

/**
 * Remove the column with case insensitive name `column` 
 */
case class DeleteColumn(
  column: String
) extends PostprocessCommand
object DeleteColumn { implicit val format: Format[DeleteColumn] = Json.format }

/**
 * Delete rows specified by `rows`.  If `rows` uses positional indexing, also update the tag column
 * so that it is valid afterwards.  This means decrementing every tag value by `high-low` on rows 
 * where `tag` >= `high`
 */
case class DeleteRows(
  rows: RowSelection // <- a set of row identities
) extends Command
object DeleteRows { implicit val format: Format[DeleteRows] = Json.format }

/**
 * Add a new column with name `column` at horizontal position `position` (or at the end) if not
 * explicitly specified.  A fixed (i.e., non-computed) default value for the column may be 
 * specified.
 */
case class InsertColumn(
  column: String,
  position: Option[Int]
) extends PreprocessCommand
object InsertColumn { implicit val format: Format[InsertColumn] = Json.format }

/**
 * Add a new row.  The semantics of InsertRow are both subtle and precise, so pay attention
 * 
 * If `position` is specified, insert the row at the specified position, otherwise append the row 
 * to the current table.  If `position` is higher than the number of rows in the table, then append
 * the row to the end.
 * 
 * The optioal `tag` field may be used to reference a historical position in the sort order.  If 
 * `tag` is specified, the operation should also modify the corresponding column to ensure that the 
 * tag is valid afterwards.  That means:
 *  - If position is None, then values is treated as having the mapping `tag` -> df.size
 *  - If position is non-empty, then values is treated as having the mapping `tag` -> `position` and
 *   incrementing every tag value by 1 on rows where `tag` >= `position`
 *
 * Default values for the row are assigned as follows:
 *  - If a preceding update was defined over a range of values that includes `position`, the 
 *    value computed by the update is used.
 *  - Otherwise the value defaults to null
 */
case class InsertRow(
  position: Option[Long], // <- row position,
  tag: Option[String],
  identifier: Option[Long] = None
) extends Command
object InsertRow { implicit val format: Format[InsertRow] = Json.format }

/**
 * Reposition the specified column in the schema.
 */
case class MoveColumn(
  column: String,
  position: Int
) extends PostprocessCommand
object MoveColumn { implicit val format: Format[MoveColumn] = Json.format }

/**
 * Rename the specified column
 */
case class RenameColumn(
  column: String,
  newName: String
) extends PreprocessCommand
{
  def rename(cmp: String): String = { if(cmp.equalsIgnoreCase(column)) { newName } else { cmp } }
  def rename(a: Attribute): Expression = { if(a.name.equalsIgnoreCase(column)){ col(newName).expr } else { a } }
  def rename(rows: RowSelection): RowSelection = 
    rows match {
      case _:GlobalRowIdentifiers => rows
      case PositionRange(low, high, tag) => PositionRange(low, high, tag.map { rename(_) })
      case _:AllRows => rows
    }
  def rename(cmd: Command): Command = 
    cmd match {
      case DeleteColumn(col) => DeleteColumn(rename(col))
      case DeleteRows(rows) => DeleteRows(rename(rows))
      case InsertColumn(col, pos) => InsertColumn(rename(col), pos)
      case InsertRow(pos, tag, id) => InsertRow(pos, tag.map { rename(_) }, id)
      case MoveColumn(col, pos) => MoveColumn(rename(col), pos)
      case RenameColumn(col, repl) => RenameColumn(rename(col), repl)
      case Sort(col, asc) => Sort(rename(col), asc)
      case Update(col, rows, value) => Update(rename(col), rename(rows), new Column(value.expr.transform { case a:Attribute => rename(a) }))
      case TagRowOrder(col, context) => TagRowOrder(rename(col), context)
    }
}
object RenameColumn { implicit val format: Format[RenameColumn] = Json.format }

/**
 * Reorder tuples in the spreadsheet according to the specified column.  
 * 
 * virtualUpdates are used internally by the runtime to preserve a snapshot of the table prior to
 * the sort happening.  When the sort is executed, it should simulate the effect of all operations 
 * in virtualUpdates being applied for the purposes of sorting, but without actually modifying 
 * the values.
 */
case class Sort(
  column: String,
  ascending: Boolean = true
) extends Command
{
  def sortDataFrame(df: DataFrame) = 
    df.orderBy {
      ascending match { 
        case true => df(column).asc
        case false => df(column).desc
      }
    }
}
object Sort { implicit val format: Format[Sort] = Json.format }

/**
 * Modify the value of the specified column according to the specified expression (which may 
 * reference other attributes in the current row).  
 */
case class Update(
  column: String,
  rows: RowSelection,
  value: Column
) extends Command
{
  override def toString: String =
    s"$column[$rows] <- ${value.expr.sql}"
}
object Update { implicit val format: Format[Update] = Json.format }

/**
 * Create a new column named `column` containing each row's position in the current sort order
 */
case class TagRowOrder(
  column: String,
  context: Seq[Command]
) extends Command
object TagRowOrder { implicit val format: Format[TagRowOrder] = Json.format }