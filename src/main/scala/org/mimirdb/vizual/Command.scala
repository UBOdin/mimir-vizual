package org.mimirdb.vizual

import play.api.libs.json._

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.{ SortDirection, Ascending, Descending }

import org.mimirdb.rowids._
import org.mimirdb.vizual.types._

sealed trait Command
{
  def invalidatesSortOrder: Boolean
}

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
          }
        JsObject(
          j.as[Map[String,JsValue]] + ("type" -> JsString(t))
        )
      }
    }
  )
}

case class DeleteColumn(
  column: String
) extends Command
{
  def invalidatesSortOrder = false
}
object DeleteColumn { implicit val format: Format[DeleteColumn] = Json.format }

case class DeleteRows(
  rows: Set[RowIdentity] // <- a set of row identities
) extends Command
{
  def invalidatesSortOrder = true
  def predicate = 
    not(col(AnnotateWithRowIds.ATTRIBUTE).isin(rows.toSeq:_*))
}
object DeleteRows { implicit val format: Format[DeleteRows] = Json.format }

case class InsertColumn(
  column: String,
  position: Option[Int],
  value: SparkValue
) extends Command
{
  def invalidatesSortOrder = false
}
object InsertColumn { implicit val format: Format[InsertColumn] = Json.format }

case class InsertRow(
  position: Option[RowPosition], // <- row position,
  values: Map[String, SparkValue]
) extends Command
{
  def invalidatesSortOrder = !position.isEmpty
}
object InsertRow { implicit val format: Format[InsertRow] = Json.format }

case class MoveColumn(
  column: String,
  position: Int
) extends Command
{
  def invalidatesSortOrder = false
}
object MoveColumn { implicit val format: Format[MoveColumn] = Json.format }

case class RenameColumn(
  column: String,
  newName: String
) extends Command
{
  def invalidatesSortOrder = false
}
object RenameColumn { implicit val format: Format[RenameColumn] = Json.format }

case class Sort(
  column: String,
  ascending: Boolean = true
) extends Command
{
  def invalidatesSortOrder = true
  def sortDataFrame(df: DataFrame) = 
    df.orderBy {
      ascending match { 
        case true => df(column).asc
        case false => df(column).desc
      }
    }
}
object Sort { implicit val format: Format[Sort] = Json.format }

case class Update(
  column: String,
  rows: Option[Set[RowIdentity]], // All rows if None
  value: Column
) extends Command
{
  def invalidatesSortOrder = false
  def predicate = 
    rows match {
      case Some(explicit) => 
        col(AnnotateWithRowIds.ATTRIBUTE).isin(explicit.toSeq:_*)
      case None => 
        lit(true)
    }
  override def toString: String =
    s"$column[${rows.map { _.mkString("; ") }.getOrElse("*")}] <- ${value.expr.sql}"
}
object Update { implicit val format: Format[Update] = Json.format }
