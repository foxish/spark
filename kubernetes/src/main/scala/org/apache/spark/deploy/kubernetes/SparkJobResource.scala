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

import java.util.concurrent.Executors

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

class SparkJobResource(client: KubernetesClient) extends Logging {
  val ApiVersion = "apache.io/v1"
  val Kind = "SparkJob"

//  apiVersion: "apache.io/v1"
//  kind: "SparkJob"
//  metadata:
//    name: "spark-job-1"
//  spec:
//    image: "driver-image"
//    state: "completed"
//    num-executors: 10

  val httpClient = this.getHTTPClient(client.asInstanceOf[BaseClient])
  def createJobObject(name: String, executors: Int, image: String): Unit = {
    val json = ("apiVersion" -> ApiVersion) ~
      ("kind" -> Kind) ~
      ("metadata" -> ("name" -> name)) ~
      ("spec" -> (("state" -> "started") ~
          ("image" -> image) ~
          ("num-executors" -> executors.toString)))

    val body = RequestBody.create(MediaType.parse("application/json"), compact(render(json)))
    logInfo(s"POSTing resource ${name}:" + pretty(render(json)))
    val request = new Request.Builder()
      .post(body)
      .url(s"${client.getMasterUrl()}" +
        s"apis/apache.io/v1/namespaces/default/sparkjobs/")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() > 300) {
      throw new SparkException("Unable to create SparkJob: " + response.body().string())
    }
  }

  def deleteJobObject(name: String): Unit = {
    logInfo(s"DELETEing resource ${name}")
    val request = new Request.Builder()
      .delete()
      .url(s"${client.getMasterUrl()}" +
        s"apis/apache.io/v1/namespaces/default/sparkjobs/${name}")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() > 300) {
      throw new SparkException("Unable to delete SparkJob: " + response.body().string())
    }
  }

  def updateJobObject(name: String, field: String, value: String): Unit = {
    val json = List(("op" -> "replace") ~ ("path" -> field) ~ ("value" -> value))
    val body = RequestBody.create(MediaType.parse("application/json-patch+json"),
      compact(render(json)))

    logInfo(s"PATCHing resource ${name}:" + pretty(render(json)))
    val request = new Request.Builder()
      .patch(body)
      .url(s"${client.getMasterUrl()}" +
        s"apis/apache.io/v1/namespaces/default/sparkjobs/${name}")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() > 300) {
      throw new SparkException("Unable to patch SparkJob: " + response.body().string())
    }
  }

  def findJobObject(): Unit = {
    // TODO: implement logic to find things here.
  }

  def getHTTPClient(client: BaseClient): OkHttpClient = {
    val field = classOf[BaseClient].getDeclaredField("httpClient")
    try {
      field.setAccessible(true)
      return field.get(client).asInstanceOf[OkHttpClient]
    } finally {
      field.setAccessible(false)
    }
  }
}
