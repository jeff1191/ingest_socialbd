package ucm.socialbd.com.utils

import java.text.SimpleDateFormat
import java.util.Locale

/**
  * Created by Jeff on 22/05/2017.
  */
object DateUtil {

  def getDateFormatted(date:String): String ={
    val formattedDate = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US).parse(date)
     new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(formattedDate)
  }
}
