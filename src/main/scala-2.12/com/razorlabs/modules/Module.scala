package com.razorlabs.modules

import com.razorlabs.config.SparkAware
import com.razorlabs.config.props.PropsAware

import java.util.concurrent.Executors
import com.razorlabs.tools.MyLogger.logger
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

abstract class Module(val name: String) extends SparkAware with PropsAware {
  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def flow(): Unit

  val isStreaming: Boolean

  def run(): Unit = {
    val start: Long = System.currentTimeMillis()
    flow()
    shutdown(start)
  }

  def shutdown(start: Long): Unit = {
    if (isStreaming) {
      SparkAware.listenForExit()
    }
    val finish = Math.ceil((System.currentTimeMillis() - start) / 1000D / 60).toLong
    logger.info(s"Finished in $finish minutes")
    spark.stop()
    sys.exit(0)
  }

}

