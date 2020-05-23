package org.mimirdb.vizual.live

case class Box(
  column: String,
  rows: Set[Long]
)
{

  def isEmpty = rows.isEmpty
  def isNotEmpty = !rows.isEmpty

  /**
   * Test for intersection between boxes
   * @param other  The box to compare with
   * @return       True if this and [[other]] contain at least one cell in common
   */
  def intersects(other: Box) =
    column.equals(other.column) && !(rows & other.rows).isEmpty

  /**
   * Test whether a cell is contained in the box
   */
  def contains(cell: (String, Long)) =
    column.equals(cell._1) && rows(cell._2)

  /**
   * Computes the intersecting region of both boxes.  May return an empty box
   */
  def &(other: Box): Box =
    if(column.equals(other.column)){
      Box(column, rows & other.rows)
    } else { Box(column, Set.empty) }

  /**
   * Computes the region of this box not in the other box.  
   */
  def -(other: Box): Box =
    if(column.equals(other.column)){
      Box(column, rows -- other.rows)
    } else { this }

}
