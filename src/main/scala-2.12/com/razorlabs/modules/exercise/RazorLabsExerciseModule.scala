package com.razorlabs.modules.exercise

import com.razorlabs.config.props.PropsAware
import com.razorlabs.modules.Module
import com.razorlabs.modules.exercise.model.{Metadata, ParsedData, RawData}
import com.razorlabs.persistences.{FileSystemPersistence, Persistence}
import com.razorlabs.tools.{Utils, YAML}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{collect_list, explode, lag, round, row_number, udf, unix_timestamp, when}
import org.apache.spark.sql.types.TimestampType

class RazorLabsExerciseModule(name: String) extends Module(name) with PropsAware {
  val s = SparkSession.builder().getOrCreate()

  import s.implicits._

  override val isStreaming: Boolean = false

  val jobConfig: RazorLabsExerciseModuleConfig = {

    println("*** in jobConfig ***")
    val text = Utils.file.readFileContents(props.params("config-file"))
    Utils.out.prettyPrintText(text)
    YAML.mapper.readValue(text, classOf[RazorLabsExerciseModuleConfig])
  }

  val dataPersistence: Persistence[_] = FileSystemPersistence.from(jobConfig.data)
  val metadataPersistence: Persistence[_] = FileSystemPersistence.from(jobConfig.metadata)
  val mappingsPersistence: Persistence[_] = FileSystemPersistence.from(jobConfig.mapping)
  val outputPersistence: Persistence[_] = FileSystemPersistence.from(jobConfig.output)

  override def flow(): Unit = {
    val inputDs: Dataset[ParsedData] = dataPersistence.read()
      .withColumn("time", unix_timestamp($"time", "dd-MMM-yyyy h:mm:ss.SSS a").cast(TimestampType))
      .as[RawData]
      .map(_.parseData)
    val metadataDs = metadataPersistence.read().as[Metadata]
    val mappingDf = mappingsPersistence.read()

    val interpolatedDf = getInterpolatedData(inputDs)
    val enrichedDs = interpolatedDf.join(metadataDs.join(mappingDf, Seq("machine_type")), Seq("plc_name"))
    outputPersistence.save(enrichedDs)
  }

  def getInterpolatedData(data: Dataset[ParsedData]): DataFrame = {
    //Getting round time (For all 5 sec) for all filtered rows. Example 2017-04-07 00:00:02 -> 2017-04-07 00:00:00
    val timedDf = {
      val ds = data
        .filter(r => r.isInputReadingType && r.isNumericValue)
        .groupBy($"site_name", $"plc_name", $"sub_system_name", $"reading_type", $"reading_attribute", $"round_ts")
        .agg(collect_list($"value".cast(sql.types.IntegerType)).as("values"))
      ds.select("*").withColumn("avg_value", arrayAvg()($"values"))
    }

    val tsPattern = "yyyy-MM-dd HH:mm:ss"
    val win = Window.partitionBy($"site_name", $"plc_name", $"sub_system_name", $"reading_type", $"reading_attribute").orderBy($"round_ts")

    //Interpolating data for missing windows. Example we have ds of [00:00:00, 00:00:30] -> [00:00:00, 00:00:05, 00:00:10, 00:00:15, 00:00:20, 00:00:25, 00:00:30]
    val interpolatedDf = timedDf.withColumn("round_ts_prev", when(row_number.over(win) === 1, $"round_ts").otherwise(lag($"round_ts", 1).over(win)))
      .withColumn("avg_value_prev", when(row_number.over(win) === 1, $"avg_value").otherwise(lag($"avg_value", 1).over(win)))
      .withColumn("interpolatedList", tsInterpolate(tsPattern)($"round_ts_prev", $"round_ts", $"avg_value_prev", $"avg_value"))
      .withColumn("interpolated", explode($"interpolatedList"))
      .select($"site_name", $"plc_name", $"sub_system_name", $"reading_type", $"reading_attribute", $"interpolated._1".as("reading_date"), round($"interpolated._2", 2).as("value"))
    interpolatedDf
  }


  def arrayAvg(): UserDefinedFunction = udf { (input: Seq[Int]) => if (input.nonEmpty) input.sum / input.length else 0 }

  def tsInterpolate(tsPattern: String): UserDefinedFunction = udf {

    (ts1: String, ts2: String, amt1: Double, amt2: Double) =>
      import java.time.LocalDateTime
      import java.time.format.DateTimeFormatter

      val timeFormat = DateTimeFormatter.ofPattern(tsPattern)

      val perMinuteTS = if (ts1 == ts2) Vector(ts1) else {
        val ldt1 = LocalDateTime.parse(ts1, timeFormat)
        val ldt2 = LocalDateTime.parse(ts2, timeFormat)
        Iterator.iterate(ldt1.plusSeconds(5))(_.plusSeconds(5)).
          takeWhile(!_.isAfter(ldt2)).
          map(_.format(timeFormat)).
          toVector
      }

      val perMinuteAmt = for {
        i <- 1 to perMinuteTS.size
      } yield amt1 + ((amt2 - amt1) * i / perMinuteTS.size)

      perMinuteTS zip perMinuteAmt
  }
}
