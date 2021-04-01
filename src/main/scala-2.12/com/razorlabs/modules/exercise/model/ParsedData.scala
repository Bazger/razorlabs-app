package com.razorlabs.modules.exercise.model

import java.sql.Timestamp

case class ParsedData(site_name: String, plc_name: String, sub_system_name: String, reading_type: String,
                      reading_attribute: String, reading_date: Timestamp, round_ts: Timestamp, value: String) {
  val isInputReadingType: Boolean = Seq("I", "I1", "I2", "I3", "D1").contains(reading_type)
  val isNumericValue: Boolean = value.matches("[-+]?\\d+(\\.\\d+)?")
}
