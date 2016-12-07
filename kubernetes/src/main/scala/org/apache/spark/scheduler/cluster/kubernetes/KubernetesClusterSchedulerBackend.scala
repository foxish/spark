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
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
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

  // TODO: do these need mutex guarding?
  // key is executor id, value is pod name
  val pendingToPod = mutable.Map.empty[String, String] // pending startup
  val executorToPod = mutable.Map.empty[String, String] // active executors
  val shutdownToPod = mutable.Map.empty[String, String] // pending shutdown
  var executorID = 0

  val sparkImage = conf.get("spark.kubernetes.sparkImage")
  val clientJarUri = conf.get("spark.executor.jar")
  val nameSpace = conf.get(
    "spark.kubernetes.namespace",
    KubernetesClusterScheduler.defaultNameSpace)
  val executorCores = conf.getInt(
    "spark.executor.cores",
     KubernetesClusterScheduler.defaultCores)
  val executorMemory = conf.getSizeAsMb(
    "spark.executor.memory",
    KubernetesClusterScheduler.defaultMemory)

  // executor back-ends take their configuration this way
  val dynamicExecutors = Utils.isDynamicAllocationEnabled(conf)
  if (dynamicExecutors) {
    conf.setExecutorEnv("spark.dynamicAllocation.enabled", "true")
    conf.setExecutorEnv("spark.shuffle.service.enabled", "true")
  }
  conf.setExecutorEnv("spark.executor.cores", s"$executorCores")
  conf.setExecutorEnv("spark.executor.memory", s"${executorMemory}m")

  override def start(): Unit = {
    super.start()
    createExecutorPods(getInitialTargetExecutorNumber(sc.getConf))
  }

  override def stop(): Unit = {
    // Kill all executor pods indiscriminately
    killExecutorPods(pendingToPod.toVector)
    killExecutorPods(executorToPod.toVector)
    killExecutorPods(shutdownToPod.toVector)
    super.stop()
  }

  // Dynamic allocation interfaces
  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    logInfo(s"Received doRequestTotalExecutors: $requestedTotal")
    val n = executorToPod.size
    val delta = requestedTotal - n
    if (delta > 0) {
      logInfo(s"Adding $delta new executors")
      createExecutorPods(delta)
    } else if (delta < 0) {
      val d = -delta
      val idle = executorToPod.toVector.filter { case (id, _) => !scheduler.isExecutorBusy(id) }
      if (idle.length > 0) {
        logInfo(s"Shutting down ${idle.length} idle executors")
        shutdownExecutors(idle.take(d))
      }
      val r = math.max(0, d - idle.length)
      if (r > 0) {
        logInfo(s"Shutting down $r non-idle executors")
        shutdownExecutors(executorToPod.toVector.slice(n - r, n))
      }
    }
    // TODO: are there meaningful failure modes here?
    Future.successful(true)
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    logInfo(s"Received doKillExecutors")
    killExecutorPods(executorIds.map { id => (id, executorToPod(id)) })
    Future.successful(true)
  }

  private def createExecutorPods(n: Int) {
    for (i <- 1 to n) {
      executorID += 1
      executorToPod += ((executorID.toString, createExecutorPod(executorID)))
    }
  }

  def shutdownExecutors(idPodPairs: Seq[(String, String)]) {
    val active = getExecutorIds.toSet

    // Check for any finished shutting down and kill the pods
    val shutdown = shutdownToPod.toVector.filter { case (e, _) => !active.contains(e) }
    killExecutorPods(shutdown)

    // Now request shutdown for the new ones.
    // Move them from executor list to list pending shutdown
    for ((id, pod) <- idPodPairs) {
      try {
        // TODO: 'ask' returns a future - can it be used to check eventual success?
        Option(driverEndpoint).foreach(_.ask[Boolean](RemoveExecutor(id, ExecutorKilled)))
        executorToPod -= id
        shutdownToPod += ((id, pod))
      } catch {
        case e: Exception => logError(s"Error shutting down executor $id", e)
      }
    }
  }

  private def killExecutorPods(idPodPairs: Seq[(String, String)]) {
    for ((id, pod) <- idPodPairs) {
      try {
        client.pods().inNamespace(nameSpace).withName(pod).delete()
        pendingToPod -= id
        executorToPod -= id
        shutdownToPod -= id
      } catch {
        case e: Exception => logError(s"Error killing executor pod $pod", e)
      }
    }
  }

  def checkPending {
    //val t: Int = client.pods().inNamespace(nameSpace).withName("eje").get().getStatus().getPhase()
    val podStatus = pendingToPod.toVector.map { case (id, pod) =>
      (id, pod, client.pods().inNamespace(nameSpace).withName(pod).get().getStatus()) }
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
      conf.get(EXECUTOR_INSTANCES).getOrElse(numExecutors)
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
      "--cores", s"$executorCores")

    val reqs = new ResourceRequirementsBuilder()
      .withRequests(Map(
        "cpu" -> new Quantity(s"$executorCores"),
        "memory" -> new Quantity(s"${executorMemory}Mi")
        ).asJava)
      .build()

    var pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(podName)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")

      .addNewContainer().withName("spark-executor").withImage(sparkImage)
      .withImagePullPolicy("IfNotPresent")
      .withResources(reqs)
      .withCommand("/opt/executor.sh")
      .withArgs(submitArgs :_*)
      .endContainer()

      .endSpec().build()
    client.pods().inNamespace(nameSpace).withName(podName).create(pod)

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
