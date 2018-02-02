package org.apache.spark.sql.catalyst.expressions


import java.io.CharArrayWriter


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedExpressionCode
import org.apache.spark.sql.execution.datasources.json.JacksonGenerator
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.unsafe.types.UTF8String


/**
  * 将StructType类型的字段转换为Json String
  */
case class Json(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {


  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(child.dataType)


  val inputStructType: StructType = child.dataType match {
    case st: StructType => st
    case _ => StructType(Seq(StructField("col", child.dataType, child.nullable, Metadata.empty)))
  }


  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess


  // 参照 org.apache.spark.sql.DataFrame.toJSON
  // 参照 org.apache.spark.sql.execution.datasources.json.JsonOutputWriter.writeInternal
  protected override def nullSafeEval(data: Any): UTF8String = {
    val writer = new CharArrayWriter
    val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    val internalRow = child.dataType match {
      case _: StructType => data.asInstanceOf[InternalRow]
      case _ => InternalRow(data)
    }
    JacksonGenerator(inputStructType, gen)(internalRow)
    gen.flush
    gen.close
    val json = writer.toString
    UTF8String.fromString(
      child.dataType match {
        case _: StructType => json
        case _ => json.substring(json.indexOf(":") + 1, json.lastIndexOf("}"))
      })
  }


  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val writer = ctx.freshName("writer")
    val gen = ctx.freshName("gen")
    val st = ctx.freshName("st")
    val json = ctx.freshName("json")
    val typeJson = inputStructType.json
    def getDataExp(data: Any) =
      child.dataType match {
        case _: StructType => s"$data"
        case _ => s"new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(new Object[]{$data})"
      }
    def formatJson(json: String) =
      child.dataType match {
        case _: StructType => s"$json"
        case _ => s"""$json.substring($json.indexOf(":") + 1, $json.lastIndexOf("}"))"""
      }
    nullSafeCodeGen(ctx, ev, (row) => {
      s"""
         | com.fasterxml.jackson.core.JsonGenerator $gen = null;
         | try {
         |   org.apache.spark.sql.types.StructType $st = ${classOf[Json].getName}.getStructType("${typeJson.replace("\"", "\\\"")}");
         |   java.io.CharArrayWriter $writer = new java.io.CharArrayWriter();
         |   $gen = new com.fasterxml.jackson.core.JsonFactory().createGenerator($writer).setRootValueSeparator(null);
         |   org.apache.spark.sql.execution.datasources.json.JacksonGenerator.apply($st, $gen, ${getDataExp(row)});
         |   $gen.flush();
         |   String $json = $writer.toString();
         |   ${ev.value} = UTF8String.fromString(${formatJson(json)});
         | } catch (Exception e) {
         |   ${ev.isNull} = true;
         | } finally {
         |   if ($gen != null) $gen.close();
         | }
       """.stripMargin
    })
  }


}


object Json {


  val structTypeCache = collection.mutable.Map[String, StructType]() // [json, type]


  def getStructType(json: String): StructType = {
    structTypeCache.getOrElseUpdate(json, {
      println(">>>>> get StructType from json:")
      println(json)
      DataType.fromJson(json).asInstanceOf[StructType]
    })
  }


}
