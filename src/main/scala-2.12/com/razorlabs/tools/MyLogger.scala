package com.razorlabs.tools


import com.razorlabs.config.Spark
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}

object MyLogger {
  val logger: Logger = {
    val name = Spark.jobId
    println(s"Loading logger for $name")
    val in = getClass.getClassLoader.getResourceAsStream("log4j2.xml")
    if (in == null) {
      println("FAILED TO FIND log4j2.xml. EXITING...")
      sys.exit(1)
    }
    val source = new ConfigurationSource(in)
    Configurator.initialize(null, source)
      .getLogger(name)
  }
}
