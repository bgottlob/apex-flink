package org.apex

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import redis.clients.jedis.{Jedis, StreamEntry}
import java.util.concurrent.TimeUnit
import scala.util.parsing.json._

class LapEvent(sessionIdc: Long, carIndexc: Int, currentLapc: Int, lastLapTimec: Float) {
  val sessionId: Long = sessionIdc
  val carIndex: Int = carIndexc
  val currentLap: Int = currentLapc
  val lastLapTime: Float = lastLapTimec

  override def toString(): String = {
    return s"Session ${sessionId}, Car #${carIndex}, Current Lap: ${currentLap}, Last Lap Time ${lastLapTime}"
  }
}

trait Mappable {
  def toMap(): Map[String,String]
}

class PaceEvent(lapEventc: LapEvent, pacec: Float) extends Mappable {
  val lapEvent: LapEvent = lapEventc
  val pace: Float = pacec

  // Render as a map for redis streams
  def toMap(): Map[String,String] = {
    Map(
      "session_id" -> lapEvent.sessionId.toString,
      "car_index"  -> lapEvent.carIndex.toString,
      "lap"        -> (lapEvent.currentLap - 1).toString,
      "pace"       -> pace.toString
    )
  }
}

class LapChangeTrigger[W <: Window] extends Trigger[LapEvent, W] {
  override def onElement(element: LapEvent, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    val currentLap = ctx.getPartitionedState(new ValueStateDescriptor("currentLap", Types.INT))

    if (currentLap.value() == null) {
      currentLap.update(0)
    }

    if (element.currentLap > 0 && element.currentLap > currentLap.value()) {
      currentLap.update(element.currentLap)
      println(s"The lap has changed to ${currentLap.value()}")
      return TriggerResult.FIRE
    } else {
      return TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    return TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext): Unit = {}
}

object TelemetryRedisStream {

  def main(args: Array[String]): Unit = {
    val host: String = sys.env("APEX_REDIS_HOST")
    val port: Int = sys.env("APEX_REDIS_PORT").toInt
    val password: String = sys.env("APEX_REDIS_PASSWORD")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      6, // number of restart attempts
      Time.of(3, TimeUnit.SECONDS) // delay
    ))

    val lapStream: DataStream[StreamEntry] = env
      .addSource(new RedisStreamSource(host, port, password, "laps"))
      .name("lap-events")

    val pace: DataStream[PaceEvent] = lapStream
      //.filter(entry => entry.getFields.get("type") == "F1.LapDataPacket")
      .map(entry => {
        val fields = entry.getFields
        val sessionId: Long = fields.get("session_uid").toLong
        val carIndex: Int = fields.get("car_index").toInt
        val currentLapNum: Int = fields.get("current_lap_num").toInt
        val lastLapTime: Float = fields.get("last_lap_time").toFloat

        new LapEvent(sessionId, carIndex, currentLapNum, lastLapTime)
      })
      .keyBy(event => s"${event.sessionId}-${event.carIndex}")
      .window(GlobalWindows.create())
      .trigger(new LapChangeTrigger[GlobalWindow])
      .process(new LapPaceTracker)
      .name("lap-pace-tracker")

    pace.addSink(new RedisStreamSink[PaceEvent](host, port, password, "pace")).name("pace-sink")
    

    env.execute("Telemetry Redis Stream")
  }

}
