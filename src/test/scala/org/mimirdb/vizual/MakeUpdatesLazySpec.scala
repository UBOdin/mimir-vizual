package org.mimirdb.vizual

import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Descending

class MakeUpdatesLazySpec
  extends Specification
{

  "Simple Sequence" >> 
  {
    val updates = 
      MakeUpdatesLazy(Seq[Update](
        Update("A", AllRows(), lit(3)),
        Update("C", AllRows(), col("A") + col("b")),
        Update("B", AllRows(), lit(2))
      ))

    updates.map { _.column } must be equalTo(Seq("A", "B", "C"))
  }

  "Cycle Detection" >> 
  {
    MakeUpdatesLazy(Seq[Update](
      Update("A", AllRows(), col("B") + 1),
      Update("B", AllRows(), col("A") + 2),
    )) must throwA[MakeUpdatesLazy.CyclicalFormulaDependency]

    MakeUpdatesLazy(Seq[Update](
      Update("A", AllRows(), col("C") + 1),
      Update("B", AllRows(), col("A") + 2),
      Update("C", AllRows(), col("B") + 3),
    )) must throwA[MakeUpdatesLazy.CyclicalFormulaDependency]
  }

  "Non-Conflicting Semi-Cycles" >> 
  {
    val updates = 
      MakeUpdatesLazy(Seq[Update](
        /* 1 */ Update("A", GlobalRowIdentifiers.ofSource(1, 2, 3, 4, 5), col("B")),  // after 2
        /* 2 */ Update("B", GlobalRowIdentifiers.ofSource(1, 2, 3, 4, 5, 6, 7), col("C")),  // after 3, 4
        /* 3 */ Update("C", GlobalRowIdentifiers.ofSource(1, 2, 3, 4, 5, 6), lit(30)),   // anywhere
        /* 4 */ Update("C", GlobalRowIdentifiers.ofSource(7), col("A")), // after 5
        /* 5 */ Update("A", GlobalRowIdentifiers.ofSource(7, 8, 9, 10), lit(50))  // anywhere
      )).asInstanceOf[Seq[Update]]

    updates.map { _.column }
           .slice(2, 5) must be equalTo(Seq("C", "B", "A"))
    
  }
}