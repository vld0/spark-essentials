package vld0

import java.io.IOException

import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}

object PageSize {

  val connectionManager = new MultiThreadedHttpConnectionManager();
  val httpClient = new HttpClient(connectionManager)

  def call(url: String): Int = {

    val get = new GetMethod(url)
    get.setFollowRedirects(true)
    try {
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
