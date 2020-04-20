package org.mimirdb.vizual

import play.api.libs.json._

sealed trait RowIdentity
{
  def toJson: JsValue
}
sealed trait GlobalRowIdentifier extends RowIdentity

object GlobalRowIdentifier
{ 
  implicit val format = Format[GlobalRowIdentifier](
    ( JsPath.read[SourceRowIdentifier].map { _.asInstanceOf[GlobalRowIdentifier] } 
      orElse JsPath.read[InsertedRowIdentifier].map { _.asInstanceOf[GlobalRowIdentifier] }
    ),
    (_:GlobalRowIdentifier).toJson
  )
}
object RowIdentity
{ 
  implicit val format = Format[RowIdentity](
    ( JsPath.read[GlobalRowIdentifier].map { _.asInstanceOf[RowIdentity] } 
      orElse JsPath.read[RowPosition].map { _.asInstanceOf[RowIdentity] }
    ),
    (_:RowIdentity).toJson
  )
}




/**
 * Identifies the row with the explicitly specified global identifiers
 */
case class SourceRowIdentifier(source: Long) extends RowIdentity
{ def toJson = Json.toJson(this) }
object SourceRowIdentifier { implicit val format: Format[SourceRowIdentifier] = Json.format }

/**
 * Identifies a row inserted this session by the index of the insert command (0 is the first
 * insert command in the script)
 */
case class InsertedRowIdentifier(inserted: Int) extends RowIdentity
{ def toJson = Json.toJson(this) }
object InsertedRowIdentifier { implicit val format: Format[InsertedRowIdentifier] = Json.format }


/**
 * Identifies a position of the row in the current sort order.  The optional tag field can be used
 * to reference a previously specified sort order.  
 * 
 * If position is negative, it references positions from the end. 
 */ 
case class RowPosition(position: Long, tag: Option[String] = None) extends RowIdentity
{ def toJson = Json.toJson(this) }
object RowPosition { implicit val format: Format[RowPosition] = Json.format }
