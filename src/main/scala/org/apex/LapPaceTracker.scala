package org.apex

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{Types, TypeInformation, TypeHint}
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class LapPaceTracker extends ProcessWindowFunction[LapEvent, PaceEvent, String, GlobalWindow] {
  @transient private final val smoothing = 2
  @transient private final val historySize = 5

  private def addLapTime(history: List[Float], time: Float): List[Float] = {
    if (history.isEmpty) {
      return List(time)
    } else {
      return (time :: history).slice(0, historySize)
    }
  }

  override def process(key: String, context: Context, input: Iterable[LapEvent], out: Collector[PaceEvent]): Unit = {
    val event: LapEvent = input.last

    val ema = context.globalState.getState(new ValueStateDescriptor("ema", Types.FLOAT))
    val lapHistory = context.globalState.getListState(new ListStateDescriptor("lapHistory", TypeInformation.of(new TypeHint[Float]() {})))
    // When currentLap == 0, lastLapTime == 0.0, so ignore that
    if (event.currentLap > 1) {
      if (!(Option(ema.value).isEmpty)) { // Calculate exponental moving average
        val frac = smoothing / (1 + historySize)
        val pace: Float = (event.lastLapTime * frac) + (ema.value * (1 - frac))
        ema.update(pace)
        out.collect(new PaceEvent(event, pace))

      } else { // Calculate simple moving average until historySize laps have passed
        val history = addLapTime(lapHistory.get.asScala.toList.asInstanceOf[List[Float]], event.lastLapTime)
        lapHistory.update(history.asJava)
        val pace: Float = history.sum / history.length
        // Number of laps needed to calculate EMA now exists, set it so it is
        // calculated next time
        if (history.length >= historySize) ema.update(pace)
        out.collect(new PaceEvent(event, pace))
      }
    }
  }
}
