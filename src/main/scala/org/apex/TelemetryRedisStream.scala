package org.apex

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source._
import redis.clients.jedis.{Jedis, StreamEntry, StreamEntryID}
import java.util.AbstractMap.SimpleImmutableEntry
import scala.collection.JavaConverters._

class RedisStreamSource(host: String, port: Int) extends SourceFunction[StreamEntry] {
  private var running = true // Flag indicating whether streaming continues
  private lazy val jedis = new Jedis(host, port) // Redis connection
  private var checkpointID: StreamEntryID = null // ID of last stream entry collected

  override def run(ctx: SourceFunction.SourceContext[StreamEntry]): Unit = {
    while (running) {
      val streamData = jedis.xread(0, 0L, new SimpleImmutableEntry("MYSTREAM", checkpointID))
      if (!streamData.isEmpty()) {
        val entries = streamData.get(0).getValue.asScala
        ctx.getCheckpointLock.synchronized {
          entries.foreach(x => {
            ctx.collect(x)
            checkpointID = x.getID()
          })
        }
      }
    }
  }

  override def cancel(): Unit = {
    running = false
  }
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
    val stream: DataStream[String] = env
      .addSource(new RedisStreamSource(host, port))
      .map { x => x.getID().toString() }
   stream print


    env.execute("Telemetry Redis Stream")
  }

}
