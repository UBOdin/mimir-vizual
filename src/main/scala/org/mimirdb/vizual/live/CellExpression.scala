package org.mimirdb.vizual.live

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{ 
  Expression,
  GenericInternalRow,
  Attribute,
  BoundReference
}
import org.apache.spark.sql.functions.expr 
import breeze.math.Field.fieldShort

/**
 * An expression defining the value of a set of cells.
 * @param expression     The expression to be evaluated.  Attribute references must have already 
 *                       been bound to a target schema.
 * @param inputs         The column/positional offset of cells referenced in [[expression]] if 
 *                       [[Left]] or the absolute rowid of some row if [[Right]]
 * @param frame          The reference frame in which the expression is evaluated.  Note that
 *                       [[BaseSheet]] defines a valid reference frame.
 */
case class CellExpression(
  expression: Expression,
  inputs: Array[(String, Either[Int, Long])],
  frame: ReferenceFrame
)
{
  def withFrame(newFrame: ReferenceFrame) = 
    CellExpression(expression, inputs, newFrame)

  def apply(row: Int, col: Int, sheet: BaseSheet) = 
  {
    // Compute the cell's position in the cell expression's reference frame
    val localPos = sheet.translateTo(frame, row)

    // collect the tuple of inputs required to evaluate the expression
    val tuple: Array[Any] = 
      inputs.map { 
        case (sourceColumn, Left(rowOffset)) => 
          // The rowOffset is given in the expression's reference frame.  To get the actual row we 
          // need to reference, compute the source row's position in the sheet's reference frame.
          sheet(
            frame.translateTo(sheet, localPos + rowOffset),
            sheet.columnIndex(sourceColumn)
          )
        case (sourceColumn, Right(sourceRowId)) => 
          sheet(
            sheet.position(sourceRowId),
            sheet.columnIndex(sourceColumn)
          )
      }

    // And use spark native evaluation to compute the desired value
    expression.eval(new GenericInternalRow(tuple))
  }
}

object CellExpression
{
  def apply(column: Column, sheet: BaseSheet):CellExpression =
  {
    val (expression, inputs) = compile(column.expr, sheet)
    CellExpression(expression, inputs, sheet)
  }
  def apply(column: String, sheet: BaseSheet):CellExpression =
    apply(expr(column), sheet)

  val FIELD_OFFSET = "^\\[(\\$?)([^:\\]]+):(\\$?)(-?[0-9]+)\\]$".r
  val INVALID_FIELD = "^\\[.*".r

  def parseField(attr: Attribute):(String, Either[Int, Long]) = parseField(attr.name)
  def parseField(attr: String)   :(String, Either[Int, Long]) = 
  {
    attr match {
      case FIELD_OFFSET(_, col, absoluteRow, row) => 
        absoluteRow match { 
          case "$" => (col, Right(row.toLong))
          case "" => (col, Left(row.toInt))
        }
      case INVALID_FIELD() => throw new RuntimeException(s"Invalid Field Reference: $attr")
      case _ => (attr, Left(0))
    }
  }

  def compile(expression: Expression, sheet: BaseSheet): (Expression, Array[(String, Either[Int, Long])]) =
  {
    var fields = Seq[(String, Either[Int, Long])]()
    def fieldRef(ref:(String, Either[Int, Long])): Int = {
      val c = fields.indexOf(ref)
      if(c < 0){
        fields = fields :+ ref
        return fields.size - 1
      } else { 
        return c
      }
    }
    val bound = 
      expression.transform {
        case a:Attribute => 
          {
            val target = parseField(a)

            BoundReference(
              fieldRef(target),
              sheet.dataType(target._1),
              true
            )
          }
      }
    val analyzed:Expression = ???
    
    return (analyzed, fields.toArray)
  }
}