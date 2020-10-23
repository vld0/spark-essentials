package vld0

import java.io.IOException

import com.creanga.sparktest.LazyLogging
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}
import org.apache.spark.{SparkEnv, TaskContext}

object PageSize extends LazyLogging {

  val connectionManager = new MultiThreadedHttpConnectionManager();
  val httpClient = new HttpClient(connectionManager)

  def call(url: String): Int = {

    val get = new GetMethod(url)
    get.setFollowRedirects(true)
    try {

      println(s"Processing url ${url} on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")

      //System.out.println("connectionManager="+connectionManager.toString);
      //System.out.println("httpClient="+httpClient);

      //System.out.println("processing url="+url);

      logger.info("Processing url {} on {} for partition {}", url, SparkEnv.get.executorId, TaskContext.getPartitionId());


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
