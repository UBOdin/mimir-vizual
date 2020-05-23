package org.mimirdb.vizual.live

abstract class ReferenceFrame
{
  def rowid(pos: Int):Long
  def position(rowid: Long): Int

  def translateTo(other: ReferenceFrame, pos: Int): Int =
  {
    if(other == this){ pos }
    else { 
      other.position(rowid(pos))
    }
  }

  def offset(rowid: Long, offset: Int): Long =
    this.rowid(position(rowid) + offset)

}
case class SnapshotReferenceFrame(
  positionToRow: Array[Long],
  rowToPosition: Map[Long, Int],
  offset: Int
) extends ReferenceFrame
{
  def rowid(pos: Int)       = positionToRow(pos-offset)
  def position(rowid: Long) = rowToPosition(rowid)+offset
}
object ReferenceFrame
{
  def apply(order: Array[Long]): ReferenceFrame = 
    apply(order, 0)
  def apply(order: Array[Long], offset: Int): ReferenceFrame = 
    SnapshotReferenceFrame(
      order, 
      order.zipWithIndex
           .toMap,
      offset
    )
}