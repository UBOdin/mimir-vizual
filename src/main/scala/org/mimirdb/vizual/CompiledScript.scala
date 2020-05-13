package org.mimirdb.vizual

import scala.collection.JavaConversions._

import org.apache.spark.sql.{ DataFrame, Column, Row }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.{ Literal, Expression }

import org.mimirdb.rowids.{ AnnotateWithRowIds, MergeRowIds }
import org.mimirdb.rowids.AnnotateWithSequenceNumber

class CompiledScript(
  val inputs: Seq[(String,Option[String])],                              // InsertColumn, RenameColumn
  val insertions: (Seq[InsertRow], Seq[(TagRowOrder, Seq[InsertRow])]),  // InsertRow, TagRowOrder
  val deletions: Seq[DeleteRows],                                        // DeleteRows
  val updates: Seq[Update],                                              // Update
  val sort: Option[Sort],                                                // Sort
  val outputs: Seq[String]                                               // MoveColumn, DeleteColumn
){
  val RHS_ROWID_COLUMN = "__MIMIR_RHS_ROWID"

  def apply(input: DataFrame): DataFrame = 
  {
    val spark = input.sparkSession
    var df = AnnotateWithRowIds(input)
    
    //////////////////////////// inputs ////////////////////////////
    // Preprocess the DataFrame to introduce and rename columns as needed.  In addition to
    // applying InsertColumn and RenameColumn operations, this phase also ensures that the
    // dataframe has been tagged with ROWIDs
    val ROWID = (AnnotateWithRowIds.ATTRIBUTE -> Some(AnnotateWithRowIds.ATTRIBUTE))
    df = df.select( 
      (inputs.map { 
        case (col, None) => lit(null).as(col)
        case (col, Some(source)) if col.equals(source) => df(source)
        case (col, Some(source)) => df(source).as(col)
      } :+ (
          // Pass through the annotation, but map the ID.
          new Column(MergeRowIds(df(AnnotateWithRowIds.ATTRIBUTE).expr, Literal(1)))
            .as(AnnotateWithRowIds.ATTRIBUTE)
        )
      ):_*
    )

    val schema = inputs.map { _._1 }


    //////////////////////////// insertions ////////////////////////////
    // The insertions object is designed to alternate sequences of inserted rows with 
    // the allocation of tags to initialize positional order.  To avoid needing to sort the 
    // full dataset repeatedly, we manage positional order virtually: Every TagRowOder operation
    // creates a mapping that annotates rows with their position in the global order.
    ///Two suport functions assist
    // in the process:
    // - UnionInInsertedRows does exactly what it says on the tin:  it takes a batch of rows
    //   to insert and merges them into the dataset.  The key difficulty here is renumbering 
    //   existing tags to account for insertions earlier in the process.  Except for the one 
    //   optional position/tag to insert at, we assume that the row is appended to the end of the
    //   dataset.  Note: This function assumes that Normalize has already allocated a tag field
    //   for every Insert that has a positional order.
    // - ComputeTagRowMapping is an abstraction meant to be optimized later: It determines for 
    //   a dataframe a mapping from ROWID to position in the global order.  It does this through the
    //   (currently) horrendously inefficient approach of computing the result as of the point in
    //   time when the tag was created, and annotating all rows accordingly.
    var size = df.count()
    var tags = Seq[String]()

    def UnionInInsertedRows(insertions: Seq[InsertRow])
    {
      // If we're not doing any insertions, then turn this into a no-op
      if(!insertions.isEmpty){

        // For each row to be inserted, we construct a Row object consisting of the values (all 
        // null), the ROWID (MERGE(id, 2), as compared to MERGE(ROWID, 1) for the raw data), and
        // the row's position in the tag order.  The latter defaults to the end of the DataFrame
        // (something we track with `size`, above).  We're allowed to replace *one* of these 
        // positions corresponding to `tag` with a custom-defined position.
        val insertedRows = insertions.map { case InsertRow(posMaybe, tag, id) => 
          val rowTags =
            posMaybe match {
              case None => tags.map { _ => size }
              case Some(pos) => tags.map { 
                case x if tag.get.equals(x) => pos
                case _ => size
              }
            } 
          size += 1
          Row.fromSeq(
            schema.map { null } ++
            Seq(MergeRowIds(Literal(id), Literal(2))) ++
            rowTags
          ) 
        }

        // Next we compute the offsets to the existing position maps.  To do this, we pick out
        // every insertion with an explicitly defined position, group by tag, ...
        val insertPositionsByTag: Map[String, Seq[Long]] = 
          insertions.collect { 
                      case InsertRow(Some(pos), tag, _) => tag.get -> pos
                    }
                    .groupBy { _._1 }
                    .mapValues { _.map { _._2 } }
        // .. and then use a UDF closure to increment the position index by 1 for every inserted
        // row that precedes it.
        val insertCorrectionsByTag: Map[String, Column] =
          insertPositionsByTag.map { case (tag, positions) => 
            val fixPosition = 
              udf { oldPos: Long => positions.foldLeft(oldPos) { (curr, cmp) => 
                                        if(curr >= cmp) { curr + 1 } else { curr } } }
            tag -> fixPosition(df(tag))
          }.toMap

        // If any offsets are required, then apply them.  If not, skip this step.
        if(!insertCorrectionsByTag.isEmpty){
          df = df.select((
            schema.map { df(_) } ++
            Seq(df(AnnotateWithRowIds.ATTRIBUTE)) ++
            tags.map { tag => insertCorrectionsByTag.getOrElse(tag, df(tag)) }
          ):_*)
        }

        // Finally union the inserted rows with the data frame (remember that actual order won't
        // matter until later, when we apply the final sort.
        df = df.union(spark.createDataFrame(insertedRows, df.schema))
      }
    }

    // Fold in the first batch of insertions
    UnionInInsertedRows(insertions._1)

    // And then for each tag/insert pair
    for( (TagRowOrder(column, context), insertBatch) <- insertions._2 ){

      // Apply the tag
      val tagMap: DataFrame = ComputeTagRowMapping(context, input, column)
      df = df.join(
        right = tagMap,
        joinExprs = df(AnnotateWithRowIds.ATTRIBUTE) === tagMap(RHS_ROWID_COLUMN),
        joinType = "left"
      )
      df = df.select((
        (schema ++ Seq(AnnotateWithRowIds.ATTRIBUTE) ++ tags).map { df(_) } :+ df(column)
      ):_*)

      // Update our internal list of applied tags
      tags = tags :+ column

      // and finally insert the next batch of rows
      UnionInInsertedRows(insertBatch)
    }

    //////////////////////////// deletions ////////////////////////////
    // Deletions simply specify a set of rows by identity.  Delete them
    for(delete <- deletions){
      delete.rows match {
        case AllRows() => {
          df = spark.createDataFrame(new java.util.ArrayList[Row](), df.schema)
        }
        case _ => df.filter { not(delete.rows.predicate) }
      }
    }

    //////////////////////////// updates ////////////////////////////
    // Updates give literal expressions.  Deploy!
    for(Update(column, rows, value) <- updates){
      df = df.select( 
        df.schema.fieldNames.map { 
          case field if field.equals(column) => {
            rows match {
              case AllRows() => value
              case _ => when(rows.predicate, value).otherwise(df(field))
              }
          }
          case field => df(field)
        }:_*
      )
    }

    //////////////////////////// sort ////////////////////////////
    // If we have a sort, then apply it
    for(Sort(sortColumn, asc) <- sort){
      val col = df(sortColumn)
      df = df.sort(if(asc) { col.asc } else { col.desc })
    }

    //////////////////////////// outputs ////////////////////////////
    // Finally project away unwanted columns
    df = df.select(outputs.map { df(_) }:_*)

    //////////////////////////// done ////////////////////////////
    // Return the final result
    return df
  }

  def ComputeTagRowMapping(context: Seq[Command], input: DataFrame, columnName: String): DataFrame =
  {
    val script = Normalize(context, input.schema)
    val df = script(input)
    AnnotateWithSequenceNumber(df, columnName)
       .select(
         df(AnnotateWithRowIds.ATTRIBUTE).as(RHS_ROWID_COLUMN),
         df(columnName)
       )

  }
}