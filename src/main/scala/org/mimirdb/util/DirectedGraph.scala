package org.mimirdb.util

import scala.collection.mutable.Buffer
import scala.collection.mutable.{ Map => MutableMap }
import scala.collection.mutable.{ Set => MutableSet }

class DirectedGraph[V,E]()
{
  type Vertex = Int
  private val vertices = Buffer[Option[V]]()
  private val edges = MutableMap[Vertex,MutableMap[Vertex, E]]()
  private val reusableVertices = MutableSet[Vertex]()

  def addVertex(v:V): Vertex = 
  { 
    if(reusableVertices.isEmpty){
      vertices += Some(v)
      return vertices.size-1 
    } else {
      val idx = reusableVertices.head
      reusableVertices.remove(idx)
      vertices(idx) = Some(v)
      return idx
    }
  }
  def addEdge(a: Vertex, b: Vertex, label: E)
  { 
    if(!edges.contains(a)){ edges(a) = MutableMap() }
    edges(a)(b) = label
  }
  def dropVertex(idx: Vertex) = 
  {
    vertices(idx) = None
    reusableVertices.add(idx)
    edges.remove(idx)
    for((_, outEdges) <- edges){ outEdges.remove(idx) }
  }
  def dropEdge(a: Vertex, b: Vertex)
  {
    if(!edges.contains(a)){ return; }
    edges(a).remove(b)
    if(edges(a).size <= 0){ edges.remove(a) }
  }

  def allEdges: Iterator[(Vertex, Vertex, E)] =
    edges.iterator
         .flatMap { case (source, outEdges) => 
                      outEdges.iterator
                              .map { case (target, label) => (source, target, label) } }
  
  def outEdges(a: Vertex): Iterator[(Vertex, E)] =
    edges.get(a)
         .map { _.iterator }
         .getOrElse { Seq().iterator }

  def allVertices: Iterator[(Vertex, V)] =
    vertices.iterator
            .zipWithIndex
            .collect { case (Some(v), idx) => (idx, v) }

  def apply(idx: Vertex): V = 
    vertices(idx).get

}