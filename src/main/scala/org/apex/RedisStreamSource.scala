package org.apex

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source._
import redis.clients.jedis.{Jedis, StreamEntry, StreamEntryID}
import java.util.AbstractMap.SimpleImmutableEntry
import scala.collection.JavaConverters._

class RedisStreamSource(host: String, port: Int, stream: String) extends RichSourceFunction[StreamEntry] {
  private var running = true // Flag indicating whether streaming continues
  private var jedis: Jedis = _ // Redis connection
  private var checkpointID: StreamEntryID = null // ID of last stream entry collected

  override def open(params: Configuration): Unit = {
    jedis = new Jedis(host, port)
  }

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
    try {
      jedis.close()
    }
  }

  override def close(): Unit = {
    try {
      jedis.close()
    }
  }
}

class RedisStreamSink[A](host: String, port: Int, stream: String) extends RichSinkFunction[A] {
  private var jedis: Jedis = _ // Redis connection

  override def open(params: Configuration): Unit = {
    jedis = new Jedis(host, port)
  }

  override def close(): Unit = {
    try {
      jedis.close()
    }
  }

  override def invoke(value: A) = {
    jedis.xadd(stream, null, Map("value" -> value.toString()).asJava)
    println(s"Added value `${value}` to stream ${stream}")
  }
}
