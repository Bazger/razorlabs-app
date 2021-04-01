package com.razorlabs.config

import com.razorlabs.config.props.PropsAware

object Env extends Enumeration with PropsAware
{

  type Env = Value

  // Assigning values
  val debug: Env = Value("debug")
  val production: Env = Value("production")

  lazy val env: Env = {
      val _env = props.env
      _env match {
        case "production" =>
          production
        case "debug" | "local" =>
          debug
      }
  }


  val isDebug: Boolean = env == debug
  val isProduction: Boolean = env == production
  val user: String = {
    if (sys.env.isDefinedAt("USER"))
      sys.env("USER")
    else if (isDebug) "debug" else "production"
  }

}
