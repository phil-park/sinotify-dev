package sinotify

import java.net.URI

import com.typesafe.scalalogging
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Tool
import org.slf4j.LoggerFactory
import utils.AuthUtils

object Launcher extends Tool {

  val logger = scalalogging.Logger(LoggerFactory.getLogger("Launcher"))
  private var conf: Configuration = _
  type OptionMap = Map[Symbol, Any]

  override def getConf: Configuration = this.conf

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf
  }

  override def run(args: Array[String]): Int = {
    logger.error("Launcher started")
    val options = parseArgs(args.toList)

    val hdfsUrl = options.get('hdfsUrl) match {
      case Some(v) => v.toString
      case None =>
        logger.error("[Error] hdfs.url")
        sys.exit(1)
    }
    val uri = new URI(hdfsUrl)

    val zkConnectUrl = options.get('zkConnectUrl) match {
      case Some(v) => v.toString
      case None =>
        logger.error("[Error] zk.connect.url")
        sys.exit(1)
    }

    logger.info("hdfsUrl : " + hdfsUrl)
    logger.info("zkConnectUrl : " + zkConnectUrl)

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
      logger.info("connect Kerberos")
      logger.info("principal : " + kerberosPrincipal)
      logger.info("keytabPath : " + kerberosKeytab)
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
          logger.error("Unsupported option " + option)
          sys.exit(1)
      }
    }

    next(Map(), args)
  }
}
