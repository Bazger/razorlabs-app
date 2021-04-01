package com.razorlabs.modules.exercise

import com.razorlabs.modules.{Module, ModuleManager}

object RazorLabsExerciseModuleManager extends ModuleManager {
  override val name: String = "exercise"
  override lazy val module: Module = new RazorLabsExerciseModule(name)
}
