package org.mimirdb.vizual

import play.api.libs.json._
import org.apache.spark.sql.Column

/**
 * An expression identifying a set of rows
 */
sealed trait RowSelection
{
  def toJson: JsValue
  def canIntersect(other: RowSelection) = ???
  def predicate: Column = ???
}

object RowSelection
{ 
  implicit val format = Format[RowSelection](
    ( JsPath.read[GlobalRowIdentifiers].map { _.asInstanceOf[RowSelection] } 
      orElse JsPath.read[PositionRange].map { _.asInstanceOf[RowSelection] }
    ),
    (_:RowSelection).toJson
  )
}

/**
 * Identify rows explicitly by their global identifiers
 */
case class GlobalRowIdentifiers(ids: Set[GlobalRowIdentifier]) extends RowSelection
{ def toJson = Json.toJson(this) }
object GlobalRowIdentifiers { implicit val format: Format[GlobalRowIdentifiers] = Json.format }

/**
 * Identify a range of rows based on the current sort order starting with `low` (inclusive) and
 * ending with `high` (exclusive).  If `high` is not set, the range extends to all rows above and
 * including `low`.   If `tag` is set, use the historical sort order preserved in `tag`.
 */
case class PositionRange(low: Long, high:Option[Long], tag: Option[String] = None) extends RowSelection
{ def toJson = Json.toJson(this) }
object PositionRange { implicit val format: Format[PositionRange] = Json.format }

/**
 * Identify all rows.  Equivalent to PositionRange(0, None)
 */
case class AllRows() extends RowSelection
{ def toJson = Json.toJson(this) }
object AllRows { 
  implicit val format: Format[AllRows] = Format(
    new Reads[AllRows] { def reads(j:JsValue) = j.as[String] match {
      case "*" => JsSuccess(AllRows())
      case _ => JsError()
    }},
    new Writes[AllRows] { def writes(x:AllRows) = JsString("*") }
  )
}
