package utils

import java.text.SimpleDateFormat

object DateUtils {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val convertDateToString = (timeStamp: Long) => dateFormat.format(timeStamp)
}
