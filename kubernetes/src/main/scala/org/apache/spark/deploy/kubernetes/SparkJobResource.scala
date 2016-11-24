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

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.kubernetes.JobState
import org.apache.spark.scheduler.cluster.kubernetes.JobState._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

 /**
  * Representation of a Spark Job State in Kubernetes
  */
object SparkJobResource {
   case class Metadata(name: String, uid: Option[String] = None,
                       labels: Option[Map[String, String]] = None,
                       annotations: Option[Map[String, String]] = None)

   case class SparkJobState(apiVersion: String, kind: String,
                            metadata: Metadata, spec: Map[String, Any])

   case object JobStateSerDe extends CustomSerializer[JobState](format =>
     ( {
       case JString("SUBMITTED") => JobState.SUBMITTED
       case JString("QUEUED") => JobState.QUEUED
       case JString("RUNNING") => JobState.RUNNING
       case JString("FINISHED") => JobState.FINISHED
       case JString("KILLED") => JobState.KILLED
       case JString("FAILED") => JobState.FAILED
       case JNull => throw new UnsupportedOperationException("No JobState Specified")
     }, {
       case JobState.FAILED => JString("FAILED")
       case JobState.SUBMITTED => JString("SUBMITTED")
       case JobState.KILLED => JString("KILLED")
       case JobState.FINISHED => JString("FINISHED")
       case JobState.QUEUED => JString("QUEUED")
       case JobState.RUNNING => JString("RUNNING")
     })
   )
 }

class SparkJobResource(client: KubernetesClient) extends Logging {

  import SparkJobResource._

  implicit val formats = DefaultFormats + JobStateSerDe
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

  /*
  * using a Map as an argument here allows adding more info into the Object if needed
  * */
  def createJobObject(name: String, keyValuePairs: Map[String, Any]): Unit = {
    val resourceObject = SparkJobState(apiVersion, kind,
      Metadata(name), keyValuePairs)
    val payload = parse(write(resourceObject))
    val requestBody = RequestBody.create(MediaType.parse("application/json"),
      compact(render(payload)))
    val request = new Request.Builder()
      .post(requestBody)
      .url(apiEndpoint)
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 201) {
      logInfo(s"Successfully posted resource $name: " +
        s"${pretty(render(parse(write(resourceObject))))}")
    } else {
      val msg = s"Failed to post resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
  }

  def updateJobObject(name: String, value: String, fieldPath: String): Unit = {
    val payload = List(("op" -> "replace") ~ ("path" -> fieldPath) ~ ("value" -> value))
    val requestBody = RequestBody.create(MediaType.parse("application/json-patch+json"),
      compact(render(payload)))
    val request = new Request.Builder()
      .post(requestBody)
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 200) {
      logInfo(s"Successfully patched resource $name")
    } else {
      val msg = s"Failed to patch resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
  }

  def deleteJobObject(name: String): Unit = {
    val request = new Request.Builder()
      .delete()
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 200) {
      logInfo(s"Successfully deleted resource $name")
    } else {
      val msg = s"Failed to delete resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
  }

  def getJobObject(name: String): SparkJobState = {
    val request = new Request.Builder()
      .get()
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 200) {
      logInfo(s"Successfully retrieved resource $name")
      read[SparkJobState](response.body().string())
    } else {
      val msg = s"Failed to retrieve resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
  }

}
