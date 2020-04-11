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

  def script = new Builder(List.empty)

  def apply(script: Seq[Command], input: DataFrame) = 
    naive(script, input)

  def naive(script: Seq[Command], input: DataFrame) = 
  { 
    var df = AnnotateWithRowIds(input)
    val spark = input.queryExecution.sparkSession
    for(instruction <- script){
      logger.trace(s"Adding $instruction\n${df.queryExecution.analyzed.treeString}")

      if(instruction.invalidatesSortOrder){
        df = AnnotateWithSequenceNumber.strip(df)
      }
      instruction match {
        case DeleteColumn(col) => {
          df = df.select(
                 df.schema
                   .fieldNames
                   .filter { !_.equalsIgnoreCase(col) }
                   .map { df(_) }:_*
               )
        }
        case DeleteRow(rows) => {
          df = rows.annotateIfNeeded(df)
          df = df.filter { not(rows.predicate) }
        }
        case InsertColumn(column, positionMaybe, valuesMaybe) => {
          val expr = valuesMaybe.getOrElse { lit(null) }
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
          df = df.select( ((before :+ expr.as(column)) ++ after ++ metadataSchema):_* )
        }
        case InsertRow(None, valuesMaybe) => {
          df = df.unionAll(singleton(
            df.schema.fields,
            valuesMaybe.getOrElse { Seq() },
            spark
          ))
          logger.debug(s"UNION: \n${df.queryExecution.analyzed.treeString}")
        }
        case InsertRow(Some(position), valuesMaybe) => {
          df = AnnotateWithSequenceNumber.withSequenceNumber(df) { dfWithSeq =>
            val seqAttribute = dfWithSeq(AnnotateWithSequenceNumber.ATTRIBUTE)
            dfWithSeq.filter { seqAttribute < position }
          }.unionAll(singleton(
            df.schema.fields,
            valuesMaybe.getOrElse { Seq() },
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
        case Sort(column, order) => {
          df = df.sort(
            order match {
              case Ascending => df(column).asc
              case Descending => df(column).desc
            }
          )
        }
        case Update(column, rows, value) => {
          df = rows.annotateIfNeeded(df)
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
      }
    }
    logger.debug("==== Input DataFrame ====\n"+input.queryExecution.analyzed.treeString)
    logger.debug("==== VIzual Script ====\n"+script.mkString("\n"))
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
    values: Seq[Any], 
    spark: SparkSession
  ): DataFrame =
  {
    spark.range(1).toDF
         .select(
            schema.zipWithIndex.map { case (StructField(name, t, _, _), idx) => 
              (
                if(idx < values.size){ lit(values(idx)).cast(t) }
                else { lit(null) }
              ).as(name)
            }:_*
          )
  }
}