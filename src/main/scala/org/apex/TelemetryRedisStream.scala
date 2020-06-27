package org.apex

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source._
import redis.clients.jedis.{Jedis, StreamEntry}

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
      .addSource(new RedisStreamSource(host, port, "MYSTREAM"))
      .name("telemetry-events")

    val alerts: DataStream[Int] = stream
      .keyBy(entry => "gearchange")
      .process(new TelemetryAggregator)
      .name("telemetry-aggregator")

    alerts print

    env.execute("Telemetry Redis Stream")
  }

}
