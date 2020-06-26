package org.apex

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source._
import redis.clients.jedis.{Jedis, StreamEntry, StreamEntryID}
import java.util.AbstractMap.SimpleImmutableEntry
import scala.collection.JavaConverters._

class RedisStreamSource(host: String, port: Int, stream: String) extends SourceFunction[StreamEntry] {
  private var running = true // Flag indicating whether streaming continues
  private lazy val jedis = new Jedis(host, port) // Redis connection
  private var checkpointID: StreamEntryID = null // ID of last stream entry collected

  override def run(ctx: SourceFunction.SourceContext[StreamEntry]): Unit = {
    while (running) {
      val data = jedis.xread(0, 0L, new SimpleImmutableEntry(stream, checkpointID))
      if (!data.isEmpty()) {
        val entries = data .get(0).getValue.asScala
        ctx.getCheckpointLock.synchronized {
          entries.foreach(x => {
            ctx.collectWithTimestamp(x, x.getID().getTime())
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
