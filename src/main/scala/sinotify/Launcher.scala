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

  override def run(args: Array[String]): Int = {
    val options = parseArgs(args.toList)

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

  def parseArgs(args: List[String]) = {
    type OptionMap = Map[Symbol, Any]

    def next(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--zk.connect" :: value :: tail =>
          next(map ++ Map('zk_connect -> value), tail)
        case "--hdfs.nn" :: value :: tail =>
          next(map ++ Map('hdfs_nn -> value), tail)
        case option :: tail =>
          println("Unsupported option " + option)
          sys.exit(1)
      }
    }

    next(Map(), args)
  }

  override def close(): Unit = {
    /*
    To Do
    */
  }
}
