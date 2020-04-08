package org.mimirdb.vizual

import play.api.libs.json._

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.SortDirection

sealed trait Command
{
  def invalidatesSortOrder: Boolean
}

// object Command
// { 
//   implicit val format = Format[Command](
//     new Reads[Command] { def reads(j: JsValue) = 
//       j.as[JsObject].value("type").as[String] match {
//         case "DeleteColumn" => JsSuccess(j.as[DeleteColumn])
//         case "DeleteRow"    => JsSuccess(j.as[DeleteRow])
//         case "InsertColumn" => JsSuccess(j.as[InsertColumn])
//         case "InsertRow"    => JsSuccess(j.as[InsertRow])
//         case "MoveColumn"   => JsSuccess(j.as[MoveColumn])
//         case "RenameColumn" => JsSuccess(j.as[RenameColumn])
//         case "Sort"         => JsSuccess(j.as[Sort])
//         case "UpdateCell"   => JsSuccess(j.as[UpdateCell])
//         case _ => JsError()
//       }
//     },
//     new Writes[Command] { def writes(c: Command) =
//       { 
//         val (t: String, j:JsValue) = 
//           c match {
//             case j:DeleteColumn => "DeleteColumn" -> Json.toJson(j)
//             case j:DeleteRow    => "DeleteRow"    -> Json.toJson(j)
//             case j:InsertColumn => "InsertColumn" -> Json.toJson(j)
//             case j:InsertRow    => "InsertRow"    -> Json.toJson(j)
//             case j:MoveColumn   => "MoveColumn"   -> Json.toJson(j)
//             case j:RenameColumn => "RenameColumn" -> Json.toJson(j)
//             case j:Sort         => "Sort"         -> Json.toJson(j)
//             case j:UpdateCell   => "UpdateCell"   -> Json.toJson(j)
//           }
//         JsObject(
//           j.as[Map[String,JsValue]] + ("type" -> JsString(t))
//         )
//       }
//     }
//   )
// }

case class DeleteColumn(
  column: String
) extends Command
{
  def invalidatesSortOrder = false
}
// object DeleteColumn { implicit val format: Format[DeleteColumn] = Json.format }

case class DeleteRow(
  rows: RowSelection
) extends Command
{
  def invalidatesSortOrder = true
}
// object DeleteRow { implicit val format: Format[DeleteRow] = Json.format }

case class InsertColumn(
  column: String,
  position: Option[Int],
  values: Option[Column]
) extends Command
{
  def invalidatesSortOrder = false
}
// object InsertColumn { implicit val format: Format[InsertColumn] = Json.format }

case class InsertRow(
  position: Option[Int],
  values: Option[Seq[Column]]
) extends Command
{
  def invalidatesSortOrder = !position.isEmpty
}
// object InsertRow { implicit val format: Format[InsertRow] = Json.format }

case class MoveColumn(
  column: String,
  position: Int
) extends Command
{
  def invalidatesSortOrder = false
}
// object MoveColumn { implicit val format: Format[MoveColumn] = Json.format }

case class RenameColumn(
  column: String,
  newName: String
) extends Command
{
  def invalidatesSortOrder = false
}
// object RenameColumn { implicit val format: Format[RenameColumn] = Json.format }

case class Sort(
  column: String,
  ascending: SortDirection
) extends Command
{
  def invalidatesSortOrder = true
}
// object Sort { implicit val format: Format[Sort] = Json.format }

case class UpdateCell(
  columns: String,
  rows: RowSelection,
  values: Column
) extends Command
{
  def invalidatesSortOrder = false
}
// object UpdateCell { implicit val format: Format[UpdateCell] = Json.format }
