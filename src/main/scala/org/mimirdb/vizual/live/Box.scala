package org.mimirdb.vizual.live

case class Box(
  column: String,
  rows: Set[Long]
)
{

  /**
   * Test for intersection between boxes
   * @param other  The box to compare with
   * @return       True if this and [[other]] contain at least one cell in common
   */
  def intersects(other: Box) =
    column.equals(other.column) && !(rows & other.rows).isEmpty

  def &(other: Box): Option[Box] =
    if(column.equals(other.column)){
      Some(Box(column, rows & other.rows))
    } else { None }

  def -(other: Box): Option[Box] =
    if(column.equals(other.column)){
      if(rows.subsetOf(other.rows)){ None }
      else { Some(Box(column, rows -- other.rows)) }
    } else { Some(this) }

  def partitionWith(other: Box): (Option[Box], Option[Box], Option[Box]) =
    (this - other, this & other, other - this)
}
