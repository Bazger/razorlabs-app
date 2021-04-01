package com.razorlabs.persistences

import com.razorlabs.config.props.{FlowIO, PropsAware}
import com.razorlabs.persistences.options.{FileFormat, FileSystemPersistenceOptions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import scala.util.Try

object FileSystemPersistence extends PropsAware {
  def from(io: FlowIO): FileSystemPersistence = {
    val format = Try(FileFormat.of(io.settings("format"))).toOption
    val schema = io.schemaStruct
    val options =
      new FileSystemPersistenceOptions(path = Some(io.path),
        schema = schema,
        format = format,
        settings = Option(io.settings),
        isStreamingRead = io.isStreaming,
        coalesce = Option(io.coalesce),
        withTimestamp = Option(io.withTimestamp),
        partitions = io.partitions,
      )
    new FileSystemPersistence(options)
  }
}

class FileSystemPersistence(options: FileSystemPersistenceOptions)
  extends Persistence[FileSystemPersistenceOptions](options) {

  protected override def readBatch(opts: FileSystemPersistenceOptions): DataFrame = {
    val df = spark.read.format(opts.format.get.toString).options(opts.settings.get)

    if (opts.schema.isDefined)
      df.schema(opts.schema.get)
    val resDF = df.load(opts.path.get)
    if (opts.partitions.isDefined)
      resDF.repartition(opts.partitions.get)
    else resDF
  }

  override def saveBatch(ds: DataFrame, opts: FileSystemPersistenceOptions): Unit = {
    val format = opts.format.get.toString
    val ds2 = opts.coalesce match {
      case Some(true) => ds.repartition(1)
      case _ if format == "text" => ds.repartition(ds.count().toInt)
      case _ => ds
    }
    ds2.write
      .mode(opts.mode.getOrElse("overwrite"))
      .format(format)
      .options(opts.settings.get)
      .save(opts.destination)
  }

  protected override def readStream(opts: FileSystemPersistenceOptions): DataFrame = ???

  override protected def saveStream(df: DataFrame, opts: FileSystemPersistenceOptions): StreamingQuery = ???

  override def optionsFromDestName(fileName: Option[String]): FileSystemPersistenceOptions = new FileSystemPersistenceOptions(fileName = fileName)
}

