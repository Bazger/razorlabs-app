package com.razorlabs.modules

trait ModuleRouter {
  val modules: Seq[ModuleManager]

  def get(job: String): ModuleManager = {
    modules.find(_.name == job.toLowerCase).get
  }
}
