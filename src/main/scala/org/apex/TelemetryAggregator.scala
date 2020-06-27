package org.apex

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import redis.clients.jedis.StreamEntry
import scala.util.Try

@SerialVersionUID(1L)
class TelemetryAggregator extends KeyedProcessFunction[String, StreamEntry, Int] {
  @transient private var gearState: ValueState[java.lang.Integer] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val gearDescriptor = new ValueStateDescriptor("gear", Types.INT)
    gearState = getRuntimeContext.getState(gearDescriptor)
  }

  @throws[Exception]
  override def processElement(
    entry: StreamEntry,
    context: KeyedProcessFunction[String, StreamEntry, Int]#Context,
    collector: Collector[Int]): Unit = {

    val lastGear = gearState.value
    val currentGear = Try(entry.getFields.get("gear").toInt).getOrElse(0)

    if (currentGear > 0 && lastGear != currentGear) {
      println(s"Gear changed to ${currentGear}")
      collector.collect(currentGear)
      gearState.update(currentGear)
    }
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[String, StreamEntry, Int]#Context): Unit = {
    // Clean up state
    gearState.clear()
  }
}
