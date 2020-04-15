package org.mimirdb.util

object MergeMaps
{
  def apply[K, L, R, O](lhs: Map[K, L], rhs: Map[K, R])(merge: (Option[L], Option[R]) => O): Map[K, O] =
  {
    (lhs.keySet | rhs.keySet)
      .map { k => k -> merge(lhs.get(k), rhs.get(k)) }
      .toMap
  }

  def simple[K, V](lhs: Map[K, V], rhs: Map[K, V])(merge: (V, V) => V): Map[K, V] =
  {
    apply(lhs, rhs){
      case (None, None) => throw new IllegalArgumentException("Merging two keyless values")
      case (Some(l), None) => l
      case (None, Some(r)) => r
      case (Some(l), Some(r)) => merge(l, r)
    }
  }

  def concat[K, V](lhs: Map[K, Seq[V]], rhs: Map[K, Seq[V]]): Map[K, Seq[V]] =
    simple(lhs, rhs) { _ ++ _ }

  def lhsWins[K, V](lhs: Map[K, V], rhs: Map[K, V]): Map[K, V] =
    simple(lhs, rhs) { (a, b) => a }
}