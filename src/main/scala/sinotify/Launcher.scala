package sinotify

import java.io.Closeable
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Tool

object Launcher extends Tool with Closeable {
  private var conf: Configuration = _

  override def getConf: Configuration = this.conf

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf
  }

  override def run(args: Array[String]):Int = {
    try {
      val uri = new URI("hdfs://localhost:8020")
      // check listener need implement multithreading
      Listener.run(uri, this.conf)
      0
    }
    catch {
      case err: Throwable =>
        1
    }
  }

  override def close(): Unit = {
    /*
    To Do
    */
  }
}
