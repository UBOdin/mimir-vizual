package org.mimirdb.vizual

import org.apache.spark.sql.{ DataFrame, Column, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._

import org.mimirdb.rowids.{ AnnotateWithRowIds, AnnotateWithSequenceNumber }

object Vizual
{

  def apply(script: Seq[Command], input: DataFrame) = 
    naive(script, input)

  def naive(script: Seq[Command], input: DataFrame) = 
  { 
    var df = AnnotateWithRowIds(input)
    val spark = input.queryExecution.sparkSession
    for(instruction <- script){
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
          val originalSchema = df.schema.fieldNames.map { df(_) }
          val (before, after):(Seq[Column], Seq[Column]) = 
            positionMaybe match { 
              case Some(position) => originalSchema.toSeq.splitAt(position)
              case None => (originalSchema, Seq())
            }
          df = df.select( ((before :+ expr.as(column)) ++ after):_* )
        }
        case InsertRow(None, valuesMaybe) => {
          df.unionAll(singleton(
            df.schema.fields,
            valuesMaybe.getOrElse { Seq() },
            spark
          ))
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
                     .filter { !column.equalsIgnoreCase(_) }
                     .map { df(_) }
          val (before, after):(Seq[Column], Seq[Column]) = 
            originalSchema.toSeq.splitAt(position)
          df = df.select( ((before :+ df(column)) ++ after):_* )
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
          df.sort(
            order match {
              case Ascending => df(column).asc
              case Descending => df(column).desc
            }
          )
        }
        case UpdateCell(column, rows, value) => {
          df = rows.annotateIfNeeded(df)
          df = df.select(
            df.schema.fieldNames.map { 
              case field if column.equalsIgnoreCase(field) => 
                when(rows.predicate, value)
                  .otherwise(df(field))
              case field => df(field)
            }:_*
          )
        }
      }
    }
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
    spark.emptyDataFrame
         .select(
            schema.zipWithIndex.map { case (StructField(name, t, _, _), idx) => 
              (
                if(idx < values.size){ lit(values(idx), t) }
                else { lit(null) }
              ).as(name)
            }:_*
          )
  }
}