package org.mimirdb.util

class DirectedGraph[V,E](
  var vertices:scala.collection.mutable.Buffer[V] 
    = scala.collection.mutable.Buffer[V](),
  var edges:scala.collection.mutable.Map[Long,(E,Long)] 
    = scala.collection.mutable.Map[Long,(E,Long)]()
)
{
  def addVertex(v:V): Long = 
    { vertices += v; vertices.size-1 }
  def addEdge(a:Long, b:Long, label: E)
    { edges.put(a,(label, b)) }
}