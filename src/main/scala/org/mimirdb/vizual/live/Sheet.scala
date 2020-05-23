package org.mimirdb.vizual.live

import scala.collection.mutable.Buffer
import org.apache.spark.sql.types.StructField
import org.mimirdb.util.DirectedGraph

class Sheet(
  data: Buffer[(Seq[Any], Long)],
  fields: Seq[StructField]
) 
  extends BaseSheet(data, fields)
{
  var expressions = new DependencyGraph()


}