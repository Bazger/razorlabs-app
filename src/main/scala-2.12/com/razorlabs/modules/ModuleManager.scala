package com.razorlabs.modules

trait ModuleManager {
  val module: Module
  val name: String

  def run(): Unit = module.run()
}


