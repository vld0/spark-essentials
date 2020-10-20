package vld0

import java.io.IOException

import com.creanga.sparktest.LazyLogging
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}

object PageSize extends LazyLogging {

  val connectionManager = new MultiThreadedHttpConnectionManager();
  val httpClient = new HttpClient(connectionManager)

  def call(url: String): Int = {

    val get = new GetMethod(url)
    get.setFollowRedirects(true)
    try {

      System.out.println("processing url="+url);

      logger.error("processing url");


      httpClient.executeMethod(get)
      val content = get.getResponseBody
      return content.length

    } catch {
      case e: IOException =>
        return -1
    } finally {
      if (get!=null) {
        get.releaseConnection()
      }
    }
  }


}
