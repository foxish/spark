/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.kubernetes

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.util.control.Breaks.{break, breakable}

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3._
import okio.{Buffer, BufferedSource}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.kubernetes.JobStateSerDe

/*
 * Representation of a Spark Job State in Kubernetes
 * */
object SparkJobResource {
  case class Metadata(name: String,
                      uid: Option[String] = None,
                      labels: Option[Map[String, String]] = None,
                      annotations: Option[Map[String, String]] = None)

  case class SparkJobState(apiVersion: String,
                           kind: String,
                           metadata: Metadata,
                           spec: Map[String, Any])

  case class WatchObject(`type`: String, `object`: SparkJobState)
}

class SparkJobResource(client: KubernetesClient)(implicit ec: ExecutionContext) extends Logging {

  import SparkJobResource._

  private implicit val formats = DefaultFormats + JobStateSerDe
  private val httpClient = getHttpClient(client.asInstanceOf[BaseClient])
  private val kind = "SparkJob"
  private val apiVersion = "apache.io/v1"
  private val apiEndpoint = s"${client.getMasterUrl}/apis/$apiVersion/" +
      s"namespaces/${client.getNamespace}/sparkjobs"

  private def getHttpClient(client: BaseClient): OkHttpClient = {
    val field = classOf[BaseClient].getDeclaredField("httpClient")
    try {
      field.setAccessible(true)
      field.get(client).asInstanceOf[OkHttpClient]
    } finally {
      field.setAccessible(false)
    }
  }

  /**
   * using a Map as an argument here allows adding more info into the Object if needed
   * */
  def createJobObject(name: String, keyValuePairs: Map[String, Any]): Unit = {
    val resourceObject =
      SparkJobState(apiVersion, kind, Metadata(name), keyValuePairs)
    val payload = parse(write(resourceObject))
    val requestBody = RequestBody
      .create(MediaType.parse("application/json"), compact(render(payload)))
    val request =
      new Request.Builder().post(requestBody).url(apiEndpoint).build()
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      val msg =
        s"Failed to post resource $name. ${response.toString}. ${compact(render(payload))}"
      logError(msg)
      throw new SparkException(msg)
    }
    logInfo(s"Successfully posted resource $name: " +
        s"${pretty(render(parse(write(resourceObject))))}")
  }

  def updateJobObject(name: String, value: String, fieldPath: String): Unit = {
    val payload = List(
      ("op" -> "replace") ~ ("path" -> fieldPath) ~ ("value" -> value))
    val requestBody = RequestBody.create(
      MediaType.parse("application/json-patch+json"),
      compact(render(payload)))
    val request = new Request.Builder()
      .post(requestBody)
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      val msg =
        s"Failed to patch resource $name. ${response.message()}. ${compact(render(payload))}"
      logError(msg)
      throw new SparkException(msg)
    }
    logInfo(s"Successfully patched resource $name")
  }

  def deleteJobObject(name: String): Unit = {
    val request =
      new Request.Builder().delete().url(s"$apiEndpoint/$name").build()
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      val msg =
        s"Failed to delete resource $name. ${response.message()}. $request"
      logError(msg)
      throw new SparkException(msg)
    }
    logInfo(s"Successfully deleted resource $name")
  }

  def getJobObject(name: String): SparkJobState = {
    val request =
      new Request.Builder().get().url(s"$apiEndpoint/$name").build()
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      val msg = s"Failed to retrieve resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
    logInfo(s"Successfully retrieved resource $name")
    read[SparkJobState](response.body().string())
  }

  /**
   * This method has an helper method that blocks to watch the object.
   * The future is completed on a Delete event.
   */
  def watchJobObject(): Future[WatchObject] = {
    val promiseWatchOver = Promise[WatchObject]()
    val request =
      new Request.Builder().get().url(s"$apiEndpoint?watch=true").build()
    httpClient.newCall(request).execute() match {
      case r: Response if r.isSuccessful =>
        val deleteWatch = watchJobObjectUtil(r)
        logInfo("Starting watch on object")
        deleteWatch onComplete {
          case Success(w: WatchObject) => promiseWatchOver trySuccess w
          case Success(_) =>
            promiseWatchOver tryFailure new SparkException("Unexpected Response received")
          case Failure(e: Throwable) =>
            promiseWatchOver tryFailure new SparkException(e.getMessage)
        }
      case r: Response =>
        val msg = s"Failed to start watch on resource ${r.code()} ${r.message()}"
        logWarning(msg)
        throw new SparkException(msg)
    }
    promiseWatchOver.future
  }

  /**
   * This method has a blocking call inside it.
   * However it is wrapped in a future, so it'll take off in another thread
   */
  private def watchJobObjectUtil(response: Response): Future[WatchObject] = {
    val promiseOfJobState = Promise[WatchObject]()
    val buffer = new Buffer()
    val source: BufferedSource = response.body().source()
    Future {
      breakable {
        // This will block until there are bytes to read or the source is definitely exhausted.
        while (!source.exhausted()) {
          source.read(buffer, 8192) match {
            case -1 => cleanUpListener(source, buffer, response)
            case _ => val wo = read[WatchObject](buffer.readUtf8())
              wo match {
                case WatchObject("DELETED", _) => promiseOfJobState success wo
                  cleanUpListener(source, buffer, response)
                case WatchObject(_, _) =>
              }
          }
        }
      }
    }
    promiseOfJobState.future
  }

  private def cleanUpListener(source: BufferedSource, buffer: Buffer, response: Response): Unit = {
    buffer.close()
    source.close()
    response.close()
    break()
  }

}
