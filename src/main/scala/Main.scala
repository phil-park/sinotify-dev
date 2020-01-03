import java.net.URI

import sinotify.Listener

object Main extends App {

  override def main(args: Array[String]): Unit = {
    Listener.run(new URI("hdfs://localhost:8020"))
  }
}
