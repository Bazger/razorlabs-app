package com.razorlabs.persistences.options

import java.time.Instant

import org.apache.spark.sql.types.StructType

object FileFormat extends Enumeration {
  def of(str: String): FileFormat = values.find(_.toString == str).get

  type FileFormat = Value
  val csv: FileFormat = Value("csv")
}

class FileSystemPersistenceOptions(var path: Option[String] = None,
                                   var fileName: Option[String] = None,
                                   var schema: Option[StructType] = None,
                                   var format: Option[FileFormat.FileFormat] = None,
                                   override var isStreamingRead: Option[Boolean] = None,
                                   var settings: Option[Map[String, String]] = None,
                                   var coalesce: Option[Boolean] = None,
                                   var withTimestamp: Option[Boolean] = None,
                                   var mode: Option[String] = None,
                                   var partitions: Option[Int] = None) extends PersistenceOptions {
  def inferSchema: Boolean = settings.getOrElse(Map()).getOrElse("inferSchema", "false").toBoolean
  if (isStreamingRead.getOrElse(false)){
    if (!inferSchema && schema.isEmpty)
      throw new Exception("Streaming can't run without defined schema")
  }

  def destination: String = {
    if (!withTimestamp.getOrElse(false))
      return path.get
    val dest = fileName match {
      case Some(name) => name
      case _ =>
        Instant.now.toEpochMilli + (format match {
          case Some(extension) => "." + extension
          case _ => ""
        })
    }
    path.get + "/" + dest
  }

}

object FileSystemPersistenceOptions {
  val defaultOptions = new FileSystemPersistenceOptions(isStreamingRead = None)
}

