package org.mimirdb.vizual

import org.apache.spark.sql.{ DataFrame, Column, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._

import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.rowids.{ AnnotateWithRowIds, AnnotateWithSequenceNumber }

object Vizual
  extends LazyLogging
{

  def script = new Script(List.empty)

  def apply(script: Seq[Command], input: DataFrame) = 
  {
    val normalized = Normalize(script, input.schema)

    /////////////////// Evaluate ////////////////////////
    // native(normalized.commands, input)
  }

  def simplify(script: Seq[Command]): Seq[Command] =
    script

  def native(script: Seq[Command], input: DataFrame) = 
  { 
    var df = AnnotateWithRowIds(input)
    val spark = input.queryExecution.sparkSession

    for(instruction <- script){
      logger.trace(s"Adding $instruction\n${df.queryExecution.analyzed.treeString}")

      instruction match {
        case DeleteColumn(col) => {
          df = df.select(
                 df.schema
                   .fieldNames
                   .filter { !_.equalsIgnoreCase(col) }
                   .map { df(_) }:_*
               )
        }
        case del:DeleteRows => {
          ???
        }
        case InsertColumn(column, positionMaybe) => {
          val originalSchema = df.schema.fieldNames.filter { !_.startsWith("__MIMIR_") }
                                                   .map { df(_) } 
          val metadataSchema = df.schema.fieldNames.filter { _.startsWith("__MIMIR_") }
                                                   .map { df(_) } 
          val (before, after):(Seq[Column], Seq[Column]) = 
            positionMaybe match { 
              case Some(position) => originalSchema.toSeq.splitAt(position)
              case None => (originalSchema, Seq())
            }
          logger.trace("Insert column before: "+before.mkString(", "))
          logger.trace("Insert column after: "+after.mkString(", "))
          df = df.select( ((before :+ lit(null).as(column)) ++ after ++ metadataSchema):_* )
        }
        case InsertRow(position, tag, identifier) => {
          ???
          df = AnnotateWithSequenceNumber.withSequenceNumber(df) { dfWithSeq =>
            val seqAttribute = dfWithSeq(AnnotateWithSequenceNumber.ATTRIBUTE)
            dfWithSeq.filter { seqAttribute < position }
          }.unionAll(singleton(
            df.schema.fields,
            Map(tag.get -> ???),
            spark
          )).unionAll(
            AnnotateWithSequenceNumber.withSequenceNumber(df) { dfWithSeq =>
              val seqAttribute = dfWithSeq(AnnotateWithSequenceNumber.ATTRIBUTE)
              dfWithSeq.filter { seqAttribute >= position }
            }
          )
        }
        case MoveColumn(column, position) => {
          val originalSchema = 
            df.schema.fieldNames
                     .filter { !_.startsWith("__MIMIR_") }
                     .filter { !column.equalsIgnoreCase(_) }
                     .map { df(_) }
          val metadataSchema = 
            df.schema.fieldNames
                     .filter { _.startsWith("__MIMIR_") }
                     .map { df(_) } 
          val (before, after):(Seq[Column], Seq[Column]) = 
            originalSchema.toSeq.splitAt(position)
          df = df.select( ((before :+ df(column)) ++ after ++ metadataSchema):_* )
        }
        case RenameColumn(originalName, newName) => {
          val schema = 
            df.schema.fieldNames
                     .map { 
                        case field if field.equalsIgnoreCase(originalName) =>
                          df(field) as newName
                        case field => df(field)
                      }
          df = df.select( schema:_* )
        }
        case sort:Sort => {
          df = sort.sortDataFrame(df)
        }
        case update@Update(column, rows, value) => {
          logger.trace(s"Applying update to annotated\n${df.queryExecution.analyzed.treeString}")
          df = df.select(
            df.schema.fieldNames.map { 
              case field if column.equalsIgnoreCase(field) => 
                when(rows.predicate, value)
                  .otherwise(df(field))
                  .as(field)
              case field => df(field)
            }:_*
          )
        }
        case TagRowOrder(tag, virtualUpdates) => ???
      }
    }
    logger.debug("==== Input DataFrame ====\n"+input.queryExecution.analyzed.treeString)
    logger.debug("==== Vizual Script ====\n"+Script(script).toString)
    logger.debug("==== Output DataFrame ====\n"+df.queryExecution.analyzed.treeString)

    df.select(
      df.schema.fieldNames
        .filter { 
          case AnnotateWithSequenceNumber.ATTRIBUTE => false
          case AnnotateWithRowIds.ATTRIBUTE => false
          case _ => true
        }
        .map { df(_) }:_*
    )
  }

  def singleton(
    schema: Seq[StructField], 
    values: Map[String, Literal], 
    spark: SparkSession
  ): DataFrame =
  {
    spark.range(1).toDF
         .select(
            schema.map { case StructField(name, t, _, _) => 
              new Column(values.getOrElse(name, new Literal(null, t))).as(name)
            }:_*
          )
  }

}