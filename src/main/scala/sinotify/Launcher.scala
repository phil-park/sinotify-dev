package sinotify

import java.io.Closeable
import java.net.URI

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Tool

object Launcher extends Tool {
  private var conf: Configuration = _

  override def getConf: Configuration = this.conf

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf
  }

  override def run(args: Array[String]): Int = {
    println("Launcher started")
    val options = parseArgs(args.toList)

    val zk_connect = options.get('zk_connect).toString
    println("start connect " + zk_connect)
    val builder = CuratorFrameworkFactory.builder()
      .connectString(zk_connect)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()

    builder.start()

    val uri = new URI("hdfs://localhost:8020")

    // check listener need implement multithreading
    val listener = Listener

    sys.addShutdownHook(
      try {
        Listener.close()
      } catch {
        case err: Throwable =>
          1
      }
    )

    Listener.run(uri, this.conf)
    0
  }

  def parseArgs(args: List[String]) = {
    println("parseArgs")
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


}
