import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import sinotify.{Launcher, Listener}

object Main {

  def main(args: Array[String]): Unit = {
    val hadoopConf = new Configuration()
    val runner = ToolRunner.run(hadoopConf, Launcher, args)
  }


}


