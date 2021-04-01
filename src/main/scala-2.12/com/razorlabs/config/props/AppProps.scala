package com.razorlabs.config.props

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.util.Try

case class AppProps(args: Array[String]) {
  println(s"Initializing AppProps with ${args.mkString(",")}")
  val params: Map[String, String] = args.filter(_.startsWith("--")).map(x => {
    val param: Array[String] = {
      if (!x.contains("=")) Array(x, "true")
      else x.split("=", 2)
    }
    param(0).substring(2) -> param(1)
  }).toMap

  def get(key: String): String = params(key)

  def get(key: String, defaultValue: String): String = params.getOrElse(key, defaultValue)

  def getOption(key: String): Option[String] = params.get(key)

  def exists(key: String): Boolean = params.contains(key)

  println(s"Got params: $params")

  val jobName: String = params("name")
  val env: String = params("env")

  val appName: String = jobName + ":" + env

}

object AppProps {

  private var _props: AppProps = _
  private var _bcProps: Broadcast[AppProps] = _

  def init(args: Array[String]): AppProps = {
    _props = new AppProps(args)
    _props
  }

  def broadcast(spark: SparkSession): Broadcast[AppProps] = {
    _bcProps = spark.sparkContext.broadcast(_props)
    _bcProps
  }

  def props: AppProps = Try(_bcProps.value).getOrElse(_props)

}

trait PropsAware {
  def props: AppProps = {
    if (AppProps.props == null) {
      println("ERROR: props not initialized")
      throw new Exception("Props not initialized")
    }
    AppProps.props
  }
}

