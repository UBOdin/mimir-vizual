package org.mimirdb.vizual

import play.api.libs.json._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DataType

import org.mimirdb.spark.SparkPrimitive

object types
{
  type RowPosition = Long
  type RowIdentity = Long
  type SparkValue = Literal  


  implicit val literalFormat: Format[Literal] = Format(
    new Reads[Literal] { 
      def reads(literal: JsValue): JsResult[Literal] = {
        val fields = literal.as[Map[String,JsValue]]
        // Note, we *want* the quotation marks and escapes on the following 
        // line, since spark annoyingly hides the non-json version from us.
        val t = DataType.fromJson(fields("dataType").toString) 
        JsSuccess(
          Literal(SparkPrimitive.decode(fields("value"), t), t)
        )
      }
    },
    new Writes[Literal] {
      def writes(literal: Literal): JsValue = {
        Json.obj( 
          "dataType" -> literal.dataType.typeName,
          "value" -> SparkPrimitive.encode(literal.value, literal.dataType)
        )
      }
    }
  )

  implicit val columnFormat: Format[Column] = Format(
    new Reads[Column] { 
      def reads(expression: JsValue): JsResult[Column] = { 
        JsSuccess(
          expr(expression.as[String])
        )
      }
    },
    new Writes[Column] {
      def writes(column: Column): JsValue = {
        JsString(column.expr.sql)
      }
    }
  )
}

