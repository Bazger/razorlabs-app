package com.razorlabs.modules.exercise.model

import java.sql.Timestamp
import java.time.ZoneOffset

case class RawData(name: String, time: Timestamp, value: String) {
  def parseData: ParsedData = {
    val patterns = name.split(Array('.', '_'))
    ParsedData(patterns.headOption.orNull, patterns.lift(1).orNull, patterns.lift(2).orNull, patterns.lift(3).orNull, patterns.lift(4).orNull, time, roundTimestamp(time), value)
  }

  def roundTimestamp(ts: Timestamp): Timestamp = {
    val instant = ts.toInstant.atZone(ZoneOffset.UTC)
    val secs = instant.getSecond
    val t = Timestamp.from(instant.withSecond(secs / 5 * 5).toInstant)
    t
  }
}
