package org.mimirdb.util

import scala.collection.mutable.Buffer
import scala.collection.mutable.{ Map => MutableMap }
import scala.collection.mutable.{ Set => MutableSet }

class DirectedGraph[V]()
{
  type Vertex = Int
  private val vertices = Buffer[Option[V]]()
  private val edges = MutableMap[Vertex,MutableSet[Vertex]]()
  private val reusableVertices = MutableSet[Vertex]()

  def addVertex(v:V): Vertex = 
  { 
    val existing = vertices.indexOf(Some(v))
    if(existing >= 0){
      return existing
    } else if(reusableVertices.isEmpty){
      vertices += Some(v)
      return vertices.size-1 
    } else {
      val idx = reusableVertices.head
      reusableVertices.remove(idx)
      vertices(idx) = Some(v)
      return idx
    }
  }
  def addEdge(a: Vertex, b: Vertex)
  { 
    if(!edges.contains(a)){ edges(a) = MutableSet() }
    edges(a).add(b)
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

  def allEdges: Iterator[(Vertex, Vertex)] =
    edges.iterator
         .flatMap { case (source, outEdges) => 
                      outEdges.iterator
                              .map { (source, _) } }
  
  def outEdges(a: Vertex): Iterator[Vertex] =
    edges.get(a)
         .map { _.iterator }
         .getOrElse { Seq().iterator }

  def inEdges(a: Vertex): Iterator[Vertex] =
    edges.iterator
         .flatMap { case (source, sourceEdges) if sourceEdges(a) => Some(source) 
                    case _ => None } 
         

  def allVertices: Iterator[(Vertex, V)] =
    vertices.iterator
            .zipWithIndex
            .collect { case (Some(v), idx) => (idx, v) }

  def vertexWhere(op: V => Boolean): Option[Vertex] = 
    {
      val idx = vertices.indexWhere { 
          case None => false
          case Some(v) => op(v)
        }
      if(idx >= 0){ return Some(idx) }
      else { return None }
    }

  def closure(v: Vertex): Set[Vertex] =
  {
    outEdges(v).flatMap { closure(_) }.toSet + v
  }

  def apply(idx: Vertex): V = 
    vertexLabel(idx)

  def vertexLabel(idx: Vertex): V =
    vertices(idx).get


  def duplicate(idx: Vertex, as:V): Vertex =
  {
    val copy = addVertex(as)
    for( target <- outEdges(idx) ){
      addEdge(copy, target)
    }
    for( source <- inEdges(idx) ){
      addEdge(source, copy)
    }
    return copy
  }

  def rename(idx: Vertex, to:V)
  {
    vertices(idx) = Some(to)
  }
}