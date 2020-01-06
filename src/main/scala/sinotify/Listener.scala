package sinotify

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event
import org.apache.hadoop.hdfs.inotify.Event.EventType
import java.io.Closeable
import utils.DateUtils

object Listener extends Closeable{

  def run(host: URI, conf: Configuration): Unit = {
    val hdfsAdmin = new HdfsAdmin(host, conf)
    val eventStream = hdfsAdmin.getInotifyEventStream(0);

    while (true) {
      eventStream.take.getEvents.map(event => {
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
      }).filter {
        // test filter
        case event: Event.CreateEvent => true
        case _ => false
      }.foreach {
        event => {
          println(event.getEventType)
          event match {
            case event: Event.CreateEvent =>
              println("  ctime = " + DateUtils.convertDateToString(event.getCtime))
            case event: Event.AppendEvent =>
              println("  path = " + event.getPath)
            case event: Event.CloseEvent =>
              println("  path = " + event.getPath)
              println("  file_size = " + event.getFileSize)
              println("  timestamp = " + DateUtils.convertDateToString(event.getTimestamp))
            case event: Event.UnlinkEvent =>
              println("  path = " + event.getPath)
              println("  timestamp = " + DateUtils.convertDateToString(event.getTimestamp))
            case event: Event.RenameEvent =>
              println(event.getSrcPath + " => " + event.getDstPath)
              println("  timestamp = " + DateUtils.convertDateToString(event.getTimestamp))
            case event: Event =>
              println("  event_type = " + event.getEventType)
          }
        }
      }
    }
  }
  override def close(): Unit = {
    /*
    To Do
    */
  }
}
