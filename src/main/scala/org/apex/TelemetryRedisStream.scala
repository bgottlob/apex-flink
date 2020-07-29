package org.apex

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import redis.clients.jedis.{Jedis, StreamEntry}
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
    /*
    if (args.length != 2) {
      System.err.println("USAGE:\TelemetryRedisStream <hostname> <port>")
      return
    }

    val host = args(0)
    val port = args(1).toInt
    */

    val host = "localhost"
    val port = 6379

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[StreamEntry] = env
      .addSource(new RedisStreamSource(host, port, "telemetry"))
      .name("telemetry-events")

    /*
    val alerts: DataStream[Int] = stream
      .keyBy(entry => "gearchange")
      .process(new TelemetryAggregator)
      .name("telemetry-aggregator")
    */

    val pace: DataStream[Float] = stream
      .filter(entry => entry.getFields.get("type") == "F1.LapDataPacket")
      .map(entry => {
        val json = JSON.parseFull(entry.getFields.get("data")).get.asInstanceOf[Map[String, Any]]
        val sessionId: Long = json.get("header").get.asInstanceOf[Map[String, Any]].get("session_uid").get.asInstanceOf[Double].toLong
        val playerCarIndex = json.get("header").get.asInstanceOf[Map[String, Any]].get("player_car_index").get.asInstanceOf[Double].toInt
        val lapData = json.get("lap_data").get.asInstanceOf[List[Any]](if (playerCarIndex > 20) 0 else playerCarIndex)

        new LapEvent(
          sessionId,
          playerCarIndex,
          lapData.asInstanceOf[Map[String, Any]].get("current_lap_num").get.asInstanceOf[Double].toInt,
          lapData.asInstanceOf[Map[String, Any]].get("last_lap_time").get.asInstanceOf[Double].toFloat
        )
      })
      .keyBy(event => s"${event.sessionId}-${event.carIndex}") // TODO key by session ID
      .window(GlobalWindows.create())
      .trigger(new LapChangeTrigger[GlobalWindow])
      .process(new LapPaceTracker)
      .name("lap-pace-tracker")

   pace.addSink(new RedisStreamSink[Float](host, port, "pace")).name("pace-sink")
    

    env.execute("Telemetry Redis Stream")
  }

}
