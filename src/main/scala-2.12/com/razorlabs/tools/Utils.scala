package com.razorlabs.tools

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.file.Files
import scala.collection.JavaConverters._

object Utils {

  object file {
    def readFileContents(path: String): String = Files.readAllLines(new File(path).toPath).asScala.mkString("\n")

    def loadResource(name: String): Array[String] = {
      val in = getClass.getClassLoader.getResourceAsStream(name)
      val bin = new BufferedReader(new InputStreamReader(in))
      val res = Iterator.continually(bin.readLine).takeWhile(_ != null).map(_.trim).filter(_.nonEmpty).toArray
      bin.close()
      res
    }
  }

  def eitherOfOptions[S](opt1: Option[S], opt2: Option[S]): Option[S] = Seq(opt1, opt2).flatten.headOption

  object reflection {

    import scala.reflect.runtime.universe._

    def getVals(x: Any): Iterable[Any] = {
      val rm = scala.reflect.runtime.currentMirror
      val accessors = rm.classSymbol(x.getClass).toType.members.collect {
        case m: MethodSymbol if m.isGetter && m.isPublic => m
      }
      val instanceMirror = rm.reflect(x)
      accessors.map(acc => instanceMirror.reflectMethod(acc).apply())
    }
  }

  object out {
    def prettyPrintText(text: String): Unit = {
      val decorator = "-" * 80
      println(Seq(decorator, text, decorator).mkString("\n"))
    }
  }
}
