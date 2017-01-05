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

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.concurrent.{blocking, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks.{break, breakable}

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3._
import okio.{Buffer, BufferedSource}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import org.apache.spark.deploy.kubernetes.SparkJobResource._
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.kubernetes.JobResourceCreateCall
import org.apache.spark.scheduler.cluster.kubernetes.JobResourceRUDCalls
import org.apache.spark.scheduler.cluster.kubernetes.JobStateSerDe

/*
 * CRUD + Watch operations on a Spark Job State in Kubernetes
 * */
private[spark] object SparkJobResource {

  implicit val formats: Formats = DefaultFormats + JobStateSerDe

  val kind = "SparkJob"
  val apiVersion = "apache.io/v1"
  val apiEndpoint = s"apis/$apiVersion/namespaces/%s/sparkjobs"

  def getHttpClient(client: BaseClient): OkHttpClient = {
    val field = classOf[BaseClient].getDeclaredField("httpClient")
    try {
      field.setAccessible(true)
      field.get(client).asInstanceOf[OkHttpClient]
    } finally {
      field.setAccessible(false)
    }
  }

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


private[spark] class SparkJobCreateResource(client: KubernetesClient, namespace: String)
  extends JobResourceCreateCall with Logging {

  private val httpClient = getHttpClient(client.asInstanceOf[BaseClient])

  /**
    * Using a Map as an argument here allows adding more info into the Object if needed
    * This is currently called on the client machine. We can avoid the token use.
    * */
  override def createJobObject(name: String, keyValuePairs: Map[String, Any]): Unit = {
    val resourceObject =
      SparkJobState(apiVersion, kind, Metadata(name), keyValuePairs)
    val payload = parse(write(resourceObject))
    val requestBody = RequestBody
      .create(MediaType.parse("application/json"), compact(render(payload)))
    val request = new Request.Builder()
      .post(requestBody)
      .url(s"${client.getMasterUrl}${apiEndpoint.format(namespace)}")
      .build()
    logDebug(s"Create Request: $request")
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      response.body().close()
      val msg =
        s"Failed to post resource $name. ${response.toString}. ${compact(render(payload))}"
      logError(msg)
      throw new SparkException(msg)
    }
    response.body().close()
    logDebug(s"Successfully posted resource $name: " +
      s"${pretty(render(parse(write(resourceObject))))}")
  }
}

private[spark] class SparkJobRUDResource(
    client: KubernetesClient,
    namespace: String,
    ec: ExecutionContext) extends JobResourceRUDCalls with Logging {

  private val protocol = "https://"

  private val httpClient = getHttpClient(client.asInstanceOf[BaseClient])

  private var watchSource: BufferedSource = _

  private lazy val buffer = new Buffer()

  // Since this will be running inside a pod
  // we can access the pods token and use it with the Authorization header when
  // making rest calls to the k8s Api
  private val kubeToken = {
    val path = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token")
    val tok = Try(new String(Files.readAllBytes(path))) match {
      case Success(some) => Option(some)
      case Failure(e: Throwable) => logError(s"${e.getMessage}")
        None
    }
    tok.map(t => t).getOrElse{
      // Log a warning just in case, but this should almost certainly never happen
      logWarning("Error while retrieving pod token")
      ""
    }
  }

  // we can also get the host from the environment variable
  private val k8sServiceHost = {
    val host = Try(sys.env("KUBERNETES_SERVICE_HOST")) match {
      case Success(h) => Option(h)
      case Failure(_) => None
    }
    host.map(h => h).getOrElse{
      // Log a warning just in case, but this should almost certainly never happen
      logWarning("Error while retrieving k8s host address")
      "127.0.0.1"
    }
  }

  // the port from the environment variable
  private val k8sPort = {
    val port = Try(sys.env("KUBERNETES_PORT_443_TCP_PORT")) match {
      case Success(p) => Option(p)
      case Failure(_) => None
    }
    port.map(p => p).getOrElse{
      // Log a warning just in case, but this should almost certainly never happen
      logWarning("Error while retrieving k8s host port")
      "8001"
    }
  }

  private def executeBlocking(cb: => WatchObject): Future[WatchObject] = {
    val p = Promise[WatchObject]()
    ec.execute(new Runnable {
        override def run(): Unit = {
          try {
            p.trySuccess(blocking(cb))
          } catch {
            case e: Throwable => p.tryFailure(e)
          }
        }
      })
    p.future
  }

  // Serves as a way to interrupt to the watcher thread.
  // This closes the source the watcher is reading from and as a result triggers promise completion
  def stopWatcher(): Unit = {
    if (watchSource != null) {
      buffer.close()
      watchSource.close()
    }
  }

  override def updateJobObject(name: String, value: String, fieldPath: String): Unit = {
    val payload = List(
      ("op" -> "replace") ~ ("path" -> fieldPath) ~ ("value" -> value))
    val requestBody = RequestBody.create(
      MediaType.parse("application/json-patch+json"),
      compact(render(payload)))
    val request = new Request.Builder()
      .addHeader("Authorization", s"Bearer $kubeToken")
      .patch(requestBody)
      .url(s"$protocol$k8sServiceHost:$k8sPort/${apiEndpoint.format(namespace)}/$name")
      .build()
    logDebug(s"Update Request: $request")
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      response.body().close()
      val msg =
        s"Failed to patch resource $name. ${response.message()}. ${compact(render(payload))}"
      logError(msg)
      throw new SparkException(s"${response.code()} ${response.message()}")
    }
    response.body().close()
    logDebug(s"Successfully patched resource $name.")
  }

  override def deleteJobObject(name: String): Unit = {
    val request = new Request.Builder()
      .addHeader("Authorization", s"Bearer $kubeToken")
      .delete()
      .url(s"$protocol$k8sServiceHost:$k8sPort/${apiEndpoint.format(namespace)}/$name")
      .build()
    logDebug(s"Delete Request: $request")
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      response.body().close()
      val msg =
        s"Failed to delete resource $name. ${response.message()}. $request"
      logError(msg)
      throw new SparkException(msg)
    }
    response.body().close()
    logInfo(s"Successfully deleted resource $name")
  }

  def getJobObject(name: String): SparkJobState = {
    val request = new Request.Builder()
      .addHeader("Authorization", s"Bearer $kubeToken")
      .get()
      .url(s"$protocol$k8sServiceHost:$k8sPort/${apiEndpoint.format(namespace)}/$name")
      .build()
    logDebug(s"Get Request: $request")
    val response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      response.body().close()
      val msg = s"Failed to retrieve resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
    logInfo(s"Successfully retrieved resource $name")
    read[SparkJobState](response.body().string())
  }

  /**
   * This method has an helper method that blocks to watch the object.
   * The future is completed on a Delete event or source exhaustion.
   * This method relies on the assumption of one sparkjob per namespace
   */
  override def watchJobObject(): Future[WatchObject] = {
    val watchClient = httpClient.newBuilder().readTimeout(0, TimeUnit.MILLISECONDS).build()
    val request = new Request.Builder()
      .addHeader("Authorization", s"Bearer $kubeToken")
      .get()
      .url(s"$protocol$k8sServiceHost:$k8sPort/${apiEndpoint.format(namespace)}?watch=true")
      .build()
    logDebug(s"Watch Request: $request")
    val resp = watchClient.newCall(request).execute()
    if (!resp.isSuccessful) {
      resp.body().close()
      val msg = s"Failed to start watch on resource ${resp.code()} ${resp.message()}"
      logWarning(msg)
      throw new SparkException(msg)
    }
    logInfo(s"Starting watch on jobResource")
    watchJobObjectUtil(resp)
  }

  /**
   * This method has a blocking call - wait on SSE - inside it.
   * However it is sent off in a new thread
   */
  private def watchJobObjectUtil(response: Response): Future[WatchObject] = {
    @volatile var wo: WatchObject = null
    watchSource = response.body().source()
    executeBlocking {
      breakable {
        // This will block until there are bytes to read or the source is exhausted.
        while (!watchSource.exhausted()) {
          watchSource.read(buffer, 8192) match {
            case -1 =>
              cleanUpListener(watchSource, buffer)
              throw new SparkException("Source is exhausted and object state is unknown")
            case _ =>
              wo = read[WatchObject](buffer.readUtf8())
              wo match {
                case WatchObject("DELETED", w) =>
                  logInfo(s"${w.metadata.name} has been deleted")
                  cleanUpListener(watchSource, buffer)
                case WatchObject(e, _) => logInfo(s"$e event. Still watching")
              }
          }
        }
      }
      wo
    }
  }

  private def cleanUpListener(source: BufferedSource, buffer: Buffer): Unit = {
    buffer.close()
    source.close()
    break()
  }
}
