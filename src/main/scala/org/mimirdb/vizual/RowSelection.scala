package org.mimirdb.vizual

import play.api.libs.json._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.mimirdb.rowids.AnnotateWithRowIds

/**
 * An expression identifying a set of rows
 */
sealed trait RowSelection
{
  def toJson: JsValue
  def canIntersect(other: RowSelection) = ???
  def predicate: Column
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
{ 
  def toJson = Json.toJson(this) 
  def predicate: Column = 
    col(AnnotateWithRowIds.ATTRIBUTE).isin(ids.map { _.toLong }.toSeq:_*)
}
object GlobalRowIdentifiers { 
  implicit val format: Format[GlobalRowIdentifiers] = Json.format 
  def ofSource(ids: Long*) =
    GlobalRowIdentifiers(ids.map { SourceRowIdentifier(_) }.toSet)
  def ofInserted(ids: Long*) =
    GlobalRowIdentifiers(ids.map { InsertedRowIdentifier(_) }.toSet)
}

/**
 * Identify a range of rows based on the current sort order starting with `low` (inclusive) and
 * ending with `high` (exclusive).  If `high` is not set, the range extends to all rows above and
 * including `low`.   If `tag` is set, use the historical sort order preserved in `tag`.
 */
case class PositionRange(low: Long, high:Option[Long], tag: Option[String] = None) extends RowSelection
{ 
  def toJson = Json.toJson(this) 
  def predicate: Column = 
    high match {
      case None => col(tag.get) >= low
      case Some(h) if low > 0 => (col(tag.get) >= low) && (col(tag.get) < high)
      case Some(h) => col(tag.get) < high
    }
}
object PositionRange { implicit val format: Format[PositionRange] = Json.format }

/**
 * Identify all rows.  Equivalent to PositionRange(0, None)
 */
case class AllRows() extends RowSelection
{ 
  def toJson = Json.toJson(this) 
  def predicate: Column = lit(true)
}
object AllRows { 
  implicit val format: Format[AllRows] = Format(
    new Reads[AllRows] { def reads(j:JsValue) = j.as[String] match {
      case "*" => JsSuccess(AllRows())
      case _ => JsError()
    }},
    new Writes[AllRows] { def writes(x:AllRows) = JsString("*") }
  )
  def predicate: Column = lit(true)
}
