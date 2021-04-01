package com.razorlabs.config.props

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

case class FlowIO(`type`: String = null,
                  isStreaming: Option[Boolean] = None,
                  var partitions: Option[Int] = None,
                  var settings: Map[String, String] = null,
                  var path: String = null,
                  schemaName: String = null,
                  coalesce: Boolean = false,
                  withTimestamp: Boolean = false,
                  var schemaDDL: Option[String] = None) {

  def schemaStruct: Option[StructType] = schemaDDL.map(s => {
    val ddlSchema = StructType.fromDDL(s)
    println("Parsed schema DDL is:")
    ddlSchema.printTreeString()
    ddlSchema
  })

  lazy val saveMode: SaveMode = SaveMode.valueOf(settings.getOrElse("saveMode", "append").capitalize)
}

object FlowIO {
  lazy val emptyFlowIO: FlowIO = FlowIO()
}
