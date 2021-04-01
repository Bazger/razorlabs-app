package com.razorlabs.persistences

import com.razorlabs.config.SparkAware
import com.razorlabs.config.props.PropsAware
import com.razorlabs.persistences.options.PersistenceOptions
import com.razorlabs.tools.Utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import scala.language.postfixOps
import scala.reflect.ClassTag

abstract class Persistence[T <: PersistenceOptions : ClassTag](val options: T)
    extends SparkAware with PropsAware {

  def name: String = getClass.getName

  def isStreamingRead(options: T): Boolean = options.isStreamingRead.getOrElse(false)

  def isStreamingWrite(df: DataFrame, options: T): Boolean = df.isStreaming

  def createOpts(options: T): T = {
    val opts = Persistence.createOpts(this.options, options)
    opts
  }

  final def save(df: DataFrame, options: PersistenceOptions): Either[StreamingQuery, Unit] = {
    val opts = createOpts(options.asInstanceOf[T])
    if (isStreamingWrite(df, opts)) {
      val streamingQuery = saveStream(df, opts)
      Left(streamingQuery)
    }
    else {
      Right(saveBatch(df, opts))
    }
  }

  final def save(df: DataFrame): Unit = save(df, options)

  protected def saveStream(df: DataFrame, options: T): StreamingQuery

  protected def streamToBatch(df: DataFrame, options: T): StreamingQuery = {
    df.writeStream
        .foreachBatch((df1: DataFrame, _: Long) => {
          saveBatch(df1, options)
        })
        .start()
  }

  protected def saveBatch(df: DataFrame, options: T): Unit

  final def read(options: T): DataFrame = {
    val opts = createOpts(options)
    if (isStreamingRead(opts)) {
      readStream(opts)
    }
    else {
      readBatch(opts)
    }
  }

  final def read(): DataFrame = read(this.options)

  protected def readBatch(options: T): DataFrame

  protected def readStream(options: T): DataFrame

  def optionsFromDestName(destName: Option[String]): T
}

object Persistence {
  def createOpts[T <: PersistenceOptions](options1: T, options2: T)(implicit tag: ClassTag[T]): T = {
    val oldOptionValues = Utils.reflection.getVals(options1).toSeq.map(_.asInstanceOf[Option[_]])
    val newOptionValues = Utils.reflection.getVals(options2).toSeq.map(_.asInstanceOf[Option[_]])
    val mixedOptions = newOptionValues.zip(oldOptionValues).map(s => Utils.eitherOfOptions(s._1, s._2))
        .reverse
    tag.runtimeClass.getConstructors.head.newInstance(mixedOptions: _*).asInstanceOf[T]
  }

}
