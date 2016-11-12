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

package org.apache.spark.scheduler.cluster.kubernetes

import collection.JavaConverters._
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.extensions.JobBuilder
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.util.Random
import scala.concurrent.Future

private[spark] class KubernetesClusterSchedulerBackend(
                                                  scheduler: TaskSchedulerImpl,
                                                  sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  val client = new DefaultKubernetesClient()

  val DEFAULT_NUMBER_EXECUTORS = 2
  val sparkExecutorName = s"spark-executor-${Random.alphanumeric take 5 mkString("")}".toLowerCase()

  var executorPods = mutable.ArrayBuffer.empty[String]
  var executorID = 0

  val sparkDriverImage = sc.getConf.get("spark.kubernetes.driver.image")
  val clientJarUri = sc.getConf.get("spark.executor.jar")
  val ns = sc.getConf.get("spark.kubernetes.namespace")
  val dynamicExecutors = Utils.isDynamicAllocationEnabled(sc.getConf)

  // executor back-ends take their configuration this way
  if (dynamicExecutors) {
    sc.getConf.setExecutorEnv("spark.dynamicAllocation.enabled", "true")
    sc.getConf.setExecutorEnv("spark.shuffle.service.enabled", "true")
  }

  override def start() {
    super.start()
    createExecutorPods(getInitialTargetExecutorNumber(sc.getConf))
  }

  override def stop(): Unit = {
    deleteExecutorPods(executorPods)
    super.stop()
  }

  // Dynamic allocation interfaces
  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    logInfo(s"Received doRequestTotalExecutors: $requestedTotal")
    val delta = requestedTotal - executorPods.length
    if (delta > 0) {
      logInfo(s"Adding $delta new executor Pods")
      createExecutorPods(delta)
    } else if (delta < 0) {
      logInfo(s"Deleting ${-delta} new executor Pods")
      // TODO: What is an informed way to kill executors with the least (or zero) load?
      val killList = executorPods.slice(executorPods.length + delta, executorPods.length)
      executorPods = executorPods.take(executorPods.length + delta)
      deleteExecutorPods(killList)
    }
    // TODO: are there meaningful failure modes here?
    Future.successful(true)
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    // I don't see doKillExecutors being called in the kube context, so I'm leaving it
    // stubbed out with an error message
    logError(s"""UNIMPLEMENTED: doKillExecutors: ${executorIds.mkString(",")}""")
    Future.successful(false)
  }

  private def createExecutorPods(n: Int) {
    for (i <- 1 to n) {
      executorID += 1
      executorPods += createExecutorPod(executorID)
    }
  }

  private def deleteExecutorPods(podNames: Seq[String]) {
    for (pod <- podNames) {
      client.pods().inNamespace(ns).withName(pod).delete()
    }
  }

  def getInitialTargetExecutorNumber(conf: SparkConf,
                                     numExecutors: Int =
                                     DEFAULT_NUMBER_EXECUTORS): Int = {
    if (dynamicExecutors) {
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      val initialNumExecutors =
        Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      require(
        initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      val targetNumExecutors =
        sys.env
          .get("SPARK_EXECUTOR_INSTANCES")
          .map(_.toInt)
          .getOrElse(numExecutors)
      conf.get(EXECUTOR_INSTANCES).getOrElse(targetNumExecutors)
    }
  }

  def createExecutorPod(executorNum: Int): String = {
    // create a single k8s executor pod.
    val labelMap = Map("type" -> "spark-executor")
    val podName = s"$sparkExecutorName-$executorNum"

    val submitArgs = mutable.ArrayBuffer.empty[String]

    if (conf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      submitArgs ++= Vector(
        "dynamic-executors")
    }

    submitArgs ++= Vector("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      "--driver-url", s"$driverURL",
      "--executor-id", s"$executorNum",
      "--hostname", "localhost",
      "--app-id", "1", // TODO: change app-id per application and pass from driver.
      "--cores", "1")

    var pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(podName)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")

      .addNewContainer().withName("spark-executor").withImage(sparkDriverImage)
      .withImagePullPolicy("IfNotPresent")
      .withCommand("/opt/executor.sh")
      .withArgs(submitArgs :_*)
      .endContainer()

      .endSpec().build()
    client.pods().inNamespace(ns).withName(podName).create(pod)

    podName
  }

  protected def driverURL: String = {
    if (conf.contains("spark.testing")) {
      "driverURL"
    } else {
      RpcEndpointAddress(
        conf.get("spark.driver.host"),
        conf.get("spark.driver.port").toInt,
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    }
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var driverLogs: Option[Map[String, String]] = None
    driverLogs
  }
}
