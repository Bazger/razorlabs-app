package com.razorlabs.modules

import com.razorlabs.config.Spark
import com.razorlabs.config.props.AppProps


object AppModules extends Modules {
  override def init(args: Array[String]): ModuleManager = {
    AppProps.init(args)
    Spark.init()
    AppProps.broadcast(Spark.sparkSession)
    AppModuleRouter.get("exercise")
  }
}
