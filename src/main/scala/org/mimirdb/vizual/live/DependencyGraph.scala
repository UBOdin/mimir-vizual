package org.mimirdb.vizual.live

import org.mimirdb.util.DirectedGraph

class DependencyGraph
  extends DirectedGraph[(Box, Option[CellExpression])]
{
  def add(expression: CellExpression, target: Box) 
  {
    // Start by computing a set of cell regions (proto-boxes) that this expression would read from
    // if evaluated on the set of cells in the target boxes
    val sources:Map[String,Set[Long]] = 
      expression.inputs
                .toSeq
                .groupBy { _._1 }
                .mapValues { 
                  _.flatMap { 
                    case (_, Left(0)) => 
                      target.rows
                    case (_, Left(offset)) =>
                      target.rows.map { expression.frame.offset(_, offset) }
                    case (_, Right(absolute)) => 
                      Seq(absolute)
                  }.toSet
                }

    // Realize the actual boxes
    val sourceBoxes = 
      sources.toSeq
             .map { case (col, rows) => Box(col, rows) }

    // Align box boundaries.  After this segment, all existing boxes in the graph should be
    // contained within (or equal to) any source or target box.  Likewise, there should exist some
    // subset of boxes in the graph that combine to contain the source box.
    for(box <- (sourceBoxes :+ target)){
      alignVertexBoxes(box)
    }

    // Now that box boundaries are aligned, find the set of source and target vertices that cover
    // the source and target boxes.
    // This segment is presently implemented with minimal optimization.  If it becomes a bottleneck,
    // some indexing might help, or possibly folding this functionality into alignVertexBoxes.
    val sourceVertices = 
      allVertices.filter { case (_, (vbox, _)) => !(vbox.rows & sources(vbox.column)).isEmpty }
                 .map { _._1 }
    val targetVertices =
      allVertices.filter { case (_, (vbox, _)) => vbox intersects target }
                 .map { _._1 }

    // Finally update the edge lists
    for(target <- targetVertices){
      // As of now, target is being computed from the new expression.  Drop the previous
      // in edges...
      for(oldSource <- inEdges(target)){
        dropEdge(oldSource, target)
      }
      // .. and replace the expression associated with the cell
      rename(
        target, 
        (vertexLabel(target)._1, Some(expression))
      )
      // finally add dependency edges to the source nodes.
      for(source <- sourceVertices){
        addEdge(source, target)
      }
    }

  }

  /**
   * Ensure that vertices after this update are aligned with the current box.
   *
   * After calling this function, the following two properties are guaranteed to hold:
   * 1. All vertex boxes are either subsets of or disjoint from [[box]]. 
   * 2. [[box]] is entirely covered by vertex boxes
   * 
   * Edges are partitioned as needed to enforce this property. 
   */
  def alignVertexBoxes(box: Box)
  {
    // Find all vertices with boxes that overlap with the target box
    val overlapping = allVertices.filter { _._2._1 intersects box }
                                 .map { _._1 }
    
    // For each of these vertices, fragment the vertex along box boundaries.  This is a noop
    // if box covers vbox
    for( v <- overlapping ){ fragment(v, box) }

    // Find any portion of box that does not remain covered by v
    val uncovered:Box = 
      allVertices.map { _._2._1 }
                 .fold(box) { (b, vbox) => b - vbox }
    
    // And if any such portion remains, add a new vertex for it
    if(uncovered.isNotEmpty) { addVertex((uncovered, None)) }
  }

  /**
   * Take the box at a specified index and fragment it into the segments overlapping with and 
   * disjoint from the provided box.
   */
  def fragment(v: Vertex, box: Box)
  {
    val (vbox, vexpr) = vertexLabel(v)
    val boxOnly = box - vbox
    val shared  = box & vbox
    val vertexOnly = vbox - box

    if(!vertexOnly.isEmpty && !shared.isEmpty) {  
      rename(v, (vertexOnly, vexpr))
      duplicate(v, (shared, vexpr))
    }
  }

  def vertexForCell(cell: (String, Long)): Option[Vertex] =
    vertexWhere { _._1 contains cell } 


  def dependencies(cell: (String, Long)): Set[Vertex] =
    vertexForCell(cell) match { 
      case None => Set()
      case Some(v) => closure(v)
    }

  /**
   * List vertices that need to be updated if the box at vertex v changes
   *
   * Note: This is a *very* hacky, preliminary algorithm.  The complexity is, in the worst case, 
   * exponential in the length of the dependency chains.  There's almost certainly a more pretty 
   * depth-first traversal using some mutable structure, but I want to get it *right* before 
   * optimizing it. -Oliver (May 18, 2020)    (Who knows, exponential may even be ok?)
   */
  def updateOrder(v: Vertex): Seq[Vertex] =
  {
    val out = outEdges(v)
    val downstream = out.map { updateOrder(_) }.toSeq

    /**
     * A simple utility method to create the union of two partial orders.
     * 
     * The algorithm is not efficient, requiring O(|a|*|b|) comparisons in the worst case.  This is
     * another potential place for optimization.  
     */
    def mergeOrders(a: Seq[Vertex], b: Seq[Vertex]): Seq[Vertex] =
    {
      if(a.isEmpty){ return b }
      if(b.contains(a.head)){
        if(a.contains(b.head)){
          // If we're here, the head of both lists appears in both lists
          // the only way this is not a cyclical dependency is if the two are the same
          if(a.head == b.head){ a.head +: mergeOrders(a.tail, b.tail) }
          else { throw new RuntimeException("Cyclical dependency") }
        } else {
          // If we're here, the head of [[a]] is in [[b]].  That means we need to evaluate [[b]]'s
          // head first.
          b.head +: mergeOrders(a, b.tail)
        }
      } else {
        // If we're here, the head of [[a]] is not in [[b]].  It's safe to lead with it.
        a.head +: mergeOrders(a.tail, b)
      }
    }

    if(downstream.isEmpty){ return Seq() }
    else { 
      downstream.tail.foldLeft(downstream.head) { mergeOrders(_, _) }
    }
  }

  def triggeredUpdates(cell: (String, Long)): Seq[(Box, CellExpression)] =
    vertexForCell(cell) match {
      case None => Seq()
      case Some(v) => updateOrder(v).map { vertexLabel(_) }
                                    .collect { case (box, Some(expr)) => (box, expr) }
    }
}