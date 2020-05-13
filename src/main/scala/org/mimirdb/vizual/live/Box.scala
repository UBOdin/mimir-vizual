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
}