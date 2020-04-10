package org.mimirdb.vizual

import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Descending

class TopologicalSortSpec
  extends Specification
{
  "Simple Sequence" >> 
  {
    val updates = 
      TopologicalSort(Seq[Update](
        Update("A", PositionalRowSelection.all, lit(3)),
        Update("C", PositionalRowSelection.all, col("A") + col("b")),
        Update("B", PositionalRowSelection.all, lit(2))
      ))

    updates.map { _.column } must be equalTo(Seq("A", "B", "C"))
  }

  "Cycle Detection" >> 
  {
    TopologicalSort(Seq[Update](
      Update("A", PositionalRowSelection.all, col("B") + 1),
      Update("B", PositionalRowSelection.all, col("A") + 2),
    )) must throwA[TopologicalSort.CyclicalFormulaDependency]

    TopologicalSort(Seq[Update](
      Update("A", PositionalRowSelection.all, col("C") + 1),
      Update("B", PositionalRowSelection.all, col("A") + 2),
      Update("C", PositionalRowSelection.all, col("B") + 3),
    )) must throwA[TopologicalSort.CyclicalFormulaDependency]
  }

  "Non-Conflicting Semi-Cycles" >> 
  {
    val updates = 
      TopologicalSort(Seq[Update](
        /* 1 */ Update("A", PositionalRowSelection.to(50), col("B")),  // after 2
        /* 2 */ Update("B", PositionalRowSelection.to(75), col("C")),  // after 3, 4
        /* 3 */ Update("C", PositionalRowSelection.to(60), lit(30)),   // anywhere
        /* 4 */ Update("C", PositionalRowSelection(61, 70), col("A")), // after 5
        /* 5 */ Update("A", PositionalRowSelection(61, 100), lit(50))  // anywhere
      ))

    updates.map { _.column }
           .slice(2, 5) must be equalTo(Seq("C", "B", "A"))
    
  }
}