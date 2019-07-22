package com.flink.example.usecase

import org.apache.avro.Schema

object SchemaInfoUtil {
  def getfield(nameAndType:String): String = {
    val nt =nameAndType.split("\\:")
    getfieldBy(nt(0),nt(1))
  }

  def getfieldBy(fieldName:String, fieldType: String) = {
    String.format("{\"name\":\"%s\",\"type\":[\"%s\",\"null\"]}",fieldName,fieldType)
  }

  def avroSchemaInfo = {
    val head = "\"type\":\"record\"," + "\"name\":\"sdk\","
    val fields = String.format("\"fields\":[%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s]",
      getfield("trace_id:string"), getfield("user_id:string"))
    val schema = String.format("{%s" + "%s}",head,fields)
    schema
  }

  def getSchema(metaSchema:String) : Schema = {
    val parser = new Schema.Parser
    val schema = parser.parse(metaSchema)
    schema
  }
}
