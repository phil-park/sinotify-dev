import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import sinotify.Launcher

object Main {

  def main(args: Array[String]): Unit = {
    val hadoopConf = new Configuration()
    ToolRunner.run(hadoopConf, Launcher, args)
    sys.exit(0)
  }
}


