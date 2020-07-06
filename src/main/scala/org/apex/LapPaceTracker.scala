package org.apex

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, TypeHint}
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class LapPaceTracker extends ProcessWindowFunction[LapEvent, Float, String, GlobalWindow] {
  @transient private final val historySize = 3

  private def addLapTime(history: List[Float], time: Float): List[Float] = {
    println(historySize)
    if (history.isEmpty) {
      return List(time)
    } else {
      return (time :: history).slice(0, historySize)
    }
  }

  override def process(key: String, context: Context, input: Iterable[LapEvent], out: Collector[Float]): Unit = {
    val event = input.last
    println(s"Evaluating for last lap time: ${event}")

    val lapHistory = context.globalState.getListState(new ListStateDescriptor("lapHistory", TypeInformation.of(new TypeHint[Float]() {})))
    val history = addLapTime(lapHistory.get.asScala.toList.asInstanceOf[List[Float]], event.lastLapTime)
    println(history)
    println(historySize)

    lapHistory.update(history.asJava)
    out.collect(history.sum / history.length)
  }
}