package sinotify

import java.net.URI

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Tool
import utils.AuthUtils

object Launcher extends Tool {
  private var conf: Configuration = _
  type OptionMap = Map[Symbol, Any]

  override def getConf: Configuration = this.conf

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf
  }

  override def run(args: Array[String]): Int = {
    println("Launcher started")
    val options = parseArgs(args.toList)

    val hdfsUrl = options.get('hdfsUrl) match {
      case Some(v) => v.toString
      case None =>
        println("[Error] hdfs.url")
        sys.exit(1)
    }
    val uri = new URI(hdfsUrl)

    val zkConnectUrl = options.get('zkConnectUrl) match {
      case Some(v) => v.toString
      case None =>
        println("[Error] zk.connect.url")
        sys.exit(1)
    }

    println("hdfsUrl : " + hdfsUrl)
    println("zkConnectUrl : " + zkConnectUrl)

    val builder = CuratorFrameworkFactory.builder()
      .connectString(zkConnectUrl)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()

    builder.start()

    val kerberosKeytab = options.get('kerberosKeytab) match {
      case Some(v) => v
      case None => None
    }

    val kerberosPrincipal = options.get('kerberosPrincipal) match {
      case Some(v) => v
      case None => None
    }

    if (kerberosKeytab != None && kerberosPrincipal != None) {
      println("connect Kerberos")
      println("principal : " + kerberosPrincipal)
      println("keytabPath : " + kerberosKeytab)
      AuthUtils.authenticate(kerberosPrincipal.toString, kerberosKeytab.toString)
    }

    val outputPath = options.get('outputPath) match {
      case Some(v) => v
      case None => None
    }


    // check listener need implement multithreading
    val listener = Listener

    sys.addShutdownHook(
      try {
        listener.close()
      } catch {
        case err: Throwable =>
          1
      }
    )
    listener.run(uri, this.conf, outputPath.toString)
    0
  }

  def parseArgs(args: List[String]): OptionMap = {
    println("parseArgs")


    def next(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--zk.connect.url" :: value :: tail =>
          next(map ++ Map('zkConnectUrl -> value), tail)
        case "--hdfs.url" :: value :: tail =>
          next(map ++ Map('hdfsUrl -> value), tail)
        case "--kerberos.principal" :: value :: tail =>
          next(map ++ Map('kerberosPrincipal -> value), tail)
        case "--kerberos.keytab" :: value :: tail =>
          next(map ++ Map('kerberosKeytab -> value), tail)
        case "--output.path" :: value :: tail =>
          next(map ++ Map('outputPath -> value), tail)
        case option :: tail =>
          println("Unsupported option " + option)
          sys.exit(1)
      }
    }

    next(Map(), args)
  }
}
