package sinotify

import java.net.{URI, UnknownHostException}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event
import org.apache.hadoop.hdfs.inotify.Event.EventType
import java.io.Closeable

import com.typesafe.scalalogging
import org.slf4j.LoggerFactory
import utils.DateUtils

object Listener extends Closeable {

  val logger = scalalogging.Logger(LoggerFactory.getLogger(classOf[Closeable]))

  def downcastEvent(event: Event): Event = {
    event.getEventType match {
      case EventType.CREATE =>
        event.asInstanceOf[Event.CreateEvent]
      case EventType.UNLINK =>
        event.asInstanceOf[Event.UnlinkEvent]
      case EventType.APPEND =>
        event.asInstanceOf[Event.AppendEvent]
      case EventType.CLOSE =>
        event.asInstanceOf[Event.CloseEvent]
      case EventType.RENAME =>
        event.asInstanceOf[Event.RenameEvent]
      case _ =>
        event
    }
  }

  def toJson(event: Event): (String, JsonAST.JObject) = {
    event match {
      case event: Event.CreateEvent =>
        "event" ->
          ("type" -> event.getEventType.toString) ~
            ("path" -> event.getPath) ~
            ("time" -> DateUtils.convertDateToString(event.getCtime))
      case event: Event.AppendEvent =>
        "event" ->
          ("type" -> event.getEventType.toString) ~
            ("path" -> event.getPath)
      case event: Event.CloseEvent =>
        "event" ->
          ("type" -> event.getEventType.toString) ~
            ("path" -> event.getPath) ~
            ("fileSize" -> event.getFileSize) ~
            ("time" -> DateUtils.convertDateToString(event.getTimestamp))
      case event: Event.UnlinkEvent =>
        "event" ->
          ("type" -> event.getEventType.toString) ~
            ("path" -> event.getPath) ~
            ("time" -> DateUtils.convertDateToString(event.getTimestamp))
      case event: Event.RenameEvent =>
        "event" ->
          ("type" -> event.getEventType.toString) ~
            ("path" -> event.getSrcPath) ~
            ("dstPath" -> event.getDstPath) ~
            ("time" -> DateUtils.convertDateToString(event.getTimestamp))
      case event: Event.MetadataUpdateEvent =>
        "event" ->
          ("type" -> event.getEventType.toString) ~
            ("metaDataType" -> event.getMetadataType.toString) ~
            ("path" -> event.getPath) ~
            ("mTime" -> event.getMtime) ~
            ("aTime" -> event.getAtime)
      case event: Event =>
        "event" ->
          ("type" -> event.getEventType.toString)
    }
  }

  def filterRule(obj: (String, JsonAST.JObject)): Boolean = {
    /*
      filtering rule for test
    */
    !obj._2.values("path").toString.startsWith("/tmp") && !obj._2.values("path").toString.startsWith("/user") && obj._2.values("type").toString != "CREATE" && obj._2.values("type").toString != "APPEND"
  }

  def publish(obj: (String, JsonAST.JObject)): Unit = {
    /*
      publish method for test
    */
    println(compact(render(obj)))
  }

  def run(host: URI, conf: Configuration, outFilePath: String): Unit = {
    val hdfsAdmin = new HdfsAdmin(host, conf)
    val eventStream = hdfsAdmin.getInotifyEventStream(0);

    while (true) {
      eventStream.take.getEvents.map(
        downcastEvent
      ).map(
        toJson
      ).filter(
        filterRule
      ).foreach(json => {
        publish(json)
      })
    }
  }

  def close(): Unit = {
    /*
    To Do
    */
  }
}
