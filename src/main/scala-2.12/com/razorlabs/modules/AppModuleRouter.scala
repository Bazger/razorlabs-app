package com.razorlabs.modules

import com.razorlabs.modules.exercise.RazorLabsExerciseModuleManager

object AppModuleRouter extends ModuleRouter{
  override val modules: Seq[ModuleManager] = Seq(
    RazorLabsExerciseModuleManager
  )
}
