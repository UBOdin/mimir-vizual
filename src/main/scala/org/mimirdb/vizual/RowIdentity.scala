package org.mimirdb.vizual

sealed trait RowIdentity

case class ExplicitRowIdentity(rows: Set[String]) extends RowIdentity
case class PositionalRowIdentity(position: Int) extends RowIdentity
