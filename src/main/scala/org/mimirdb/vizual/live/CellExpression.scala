package org.mimirdb.vizual.live

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{ 
  Expression,
  GenericInternalRow
}

/**
 * An expression defining the value of a set of cells.
 * @param expression     The expression to be evaluated.  Attribute references must have already 
 *                       been bound to a target schema.
 * @param inputs         The column/positional offset of cells referenced in [[expression]].
 * @param frame          The reference frame in which the expression is evaluated.  Note that
 *                       [[Sheet]] defines a valid reference frame.
 */
case class CellExpression(
  expression: Expression,
  inputs: Array[(String, Int)],
  frame: ReferenceFrame
)
{
  def withFrame(newFrame: ReferenceFrame) = 
    CellExpression(expression, inputs, newFrame)

  def eval(row: Int, col: Int, sheet: Sheet) = 
  {
    val rowid = sheet.rowid(row)

    // Compute the cell's position in the cell expression's reference frame
    val localPos = sheet.translateTo(frame, row)

    // collect the tuple of inputs required to evaluate the expression
    val tuple: Array[Any] = 
      inputs.map { case (sourceColumn, rowOffset) => 
        // The rowOffset is given in the expression's reference frame.  To get the actual row we 
        // need to reference, compute the source row's position in the sheet's reference frame.
        sheet(
          frame.translateTo(sheet, localPos + rowOffset),
          sheet.columnIndex(sourceColumn)
        )
      }

    // And use spark native evaluation to compute the desired value
    expression.eval(new GenericInternalRow(tuple))
  }
}

