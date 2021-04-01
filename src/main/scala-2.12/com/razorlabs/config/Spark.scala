package com.razorlabs.config

import com.razorlabs.config.props.PropsAware
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Spark extends PropsAware {

  private var _jobId: String = _
  private var _sparkSession: SparkSession = _

  private def createSparkSession: SparkSession = {
    val builder = SparkSession.builder()
        .appName(props.appName)
        .config("mapred.input.dir.recursive", value = true)
        .config("spark.sql.streaming.checkpointLocation", "/tmp")
        .config("spark.sql.streaming.schemaInference", value = true)
        .config("spark.sql.streaming.fileSource.cleaner.numThreads", 4)
        .config("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
        .config("spark.sql.shuffle.partitions", "2001")
    if (Env.isDebug) {
      println("Running spark standalone")
      builder.master("local[*]")
    }
    builder.getOrCreate()
  }

  def init(): Unit = {
    _sparkSession = createSparkSession
    sparkSession.sparkContext.setLogLevel("ERROR")
    _jobId =s"razorlabs-app:${Random.alphanumeric.take(5).mkString}:${Env.user}"
    val p = getClass.getPackage
    val name = p.getImplementationTitle
    val version = p.getImplementationVersion
    println("*" * 80)
    println(Seq(s"\tApp $name", s"Env ${Env.env}", s"Version $version").mkString("\n\t"))
    println("*" * 80)
  }

  def sparkSession: SparkSession = _sparkSession

  def jobId: String = _jobId
}

trait SparkAware {
  def spark: SparkSession = Spark.sparkSession

  def sc: SparkContext = spark.sparkContext

  def exit(): Unit = {
    println("*** SparkAware.exit: Initialing shutdown of spark job...")
    SparkAware.shouldExit = true
  }
}

object SparkAware extends SparkAware {
  private var shouldExit = false

  def listenForExit(): Unit = {
    while (!spark.sparkContext.isStopped) {
      if (shouldExit) {
        println(s"*** Shutting down now ***")
        spark.streams.active.foreach(_.stop())
        spark.stop()
      } else {
        // wait 10 seconds before checking again if work is complete
        spark.streams.awaitAnyTermination(10000)
      }
    }
  }
}
