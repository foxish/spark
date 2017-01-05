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

import scala.collection.{concurrent, mutable}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success, Try}

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder, PodFluent}
import io.fabric8.kubernetes.client.DefaultKubernetesClient

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.kubernetes.SparkJobResource.WatchObject
import org.apache.spark.deploy.kubernetes.SparkJobRUDResource
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private val client = new DefaultKubernetesClient()

  private val DEFAULT_NUMBER_EXECUTORS = 2
  private val jobResourceName = conf.get("spark.kubernetes.jobResourceName", "")

  // using a concurrent TrieMap gets rid of possible concurrency issues
  // key is executor id, value is pod name
  private val executorToPod = new concurrent.TrieMap[String, String] // active executors
  private val shutdownToPod = new concurrent.TrieMap[String, String] // pending shutdown

  private val sparkImage = conf.get("spark.kubernetes.sparkImage")
  private val ns = conf.get(
    "spark.kubernetes.namespace",
    KubernetesClusterScheduler.defaultNameSpace)
  private val dynamicExecutors = Utils.isDynamicAllocationEnabled(conf)

  private implicit val resourceWatcherPool = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonFixedThreadPool(2, "resource-watcher-pool"))

  private val sparkJobResource = new SparkJobRUDResource(client, ns, resourceWatcherPool)

  private val imagePullSecret = conf.get("spark.kubernetes.imagePullSecret", "")

  private val executorBaseName =
    s"spark-executor-${Random.alphanumeric take 5 mkString ""}".toLowerCase

  private var workingWithJobResource =
    conf.get("spark.kubernetes.jobResourceSet", "false").toBoolean

  private var watcherFuture: Future[WatchObject] = _

  // executor back-ends take their configuration this way
  if (dynamicExecutors) {
    sc.getConf.setExecutorEnv("spark.dynamicAllocation.enabled", "true")
    sc.getConf.setExecutorEnv("spark.shuffle.service.enabled", "true")
  }

  override def start(): Unit = {
    super.start()
    startLogic()
    createExecutorPods(getInitialTargetExecutorNumber(sc.getConf))
  }

  private def startLogic(): Unit = {
    if (workingWithJobResource) {
      logInfo(s"Updating Job Resource with name. $jobResourceName")
      Try(sparkJobResource
        .updateJobObject(jobResourceName, JobState.SUBMITTED.toString, "/spec/state")) match {
        case Success(_) => startWatcher()
        case Failure(e: SparkException) if e.getMessage startsWith "404" =>
          logWarning(s"Possible deletion of jobResource before backend start")
          workingWithJobResource = false
        case Failure(e: SparkException) =>
          logWarning(s"SparkJob object not updated. ${e.getMessage}")
        // SparkJob should continue if this fails as discussed
        // Maybe some retry + backoff mechanism ?
      }
    }
  }

  private def startWatcher(): Unit = {
    resourceWatcherPool.execute(new Runnable {
      override def run(): Unit = {
        watcherFuture = sparkJobResource.watchJobObject()
        watcherFuture onComplete {
          case Success(w: WatchObject) if w.`type` == "DELETED" =>
            logInfo("TPR Object deleted externally. Cleaning up")
            stopUtil()
            // TODO: are there other todo's for a clean kill while job is running?
          case Success(w: WatchObject) =>
            // Log a warning just in case, but this should almost certainly never happen
            logWarning(s"Unexpected response received. $w")
            deleteJobResource()
            workingWithJobResource = false
          case Failure(e: Throwable) =>
            logWarning(e.getMessage)
            deleteJobResource()
            workingWithJobResource = false // in case watcher fails early on
        }
      }
    })
  }

  override def stop(): Unit = {
    if (workingWithJobResource) {
      sparkJobResource.stopWatcher()
      try {
        ThreadUtils.awaitResult(watcherFuture, Duration.Inf)
      } catch {
        case _ : Throwable =>
      }
    }
    stopUtil()

  }

  private def stopUtil() = {
    resourceWatcherPool.shutdown()
    // Kill all executor pods indiscriminately
    killExecutorPods(executorToPod.toVector)
    killExecutorPods(shutdownToPod.toVector)
    // TODO: pods that failed during build up due to some error are left behind.
    super.stop()
  }

  private def deleteJobResource(): Unit = {
    try {
      sparkJobResource.deleteJobObject(jobResourceName)
    } catch {
      case e: SparkException =>
        logError(s"SparkJob object not deleted. ${e.getMessage}")
      // what else do we need to do here ?
    }
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
      if (idle.nonEmpty) {
        logInfo(s"Shutting down ${idle.length} idle executors")
        shutdownExecutors(idle.take(d))
      }
      val r = math.max(0, d - idle.length)
      if (r > 0) {
        logInfo(s"Shutting down $r non-idle executors")
        shutdownExecutors(executorToPod.toVector.slice(n - r, n))
      }
    }

    // TODO: be smarter about when to update.
    if (workingWithJobResource) {
      Try(sparkJobResource
        .updateJobObject(jobResourceName, requestedTotal.toString, "/spec/num-executors")) match {
        case Success(_) => logInfo(s"Object with name: $jobResourceName updated successfully")
        case Failure(e: SparkException) =>
          logWarning(s"SparkJob Object not updated. ${e.getMessage}")
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
      executorToPod += ((i.toString, createExecutorPod(i)))
    }
  }

  def shutdownExecutors(idPodPairs: Seq[(String, String)]) {
    val active = getExecutorIds().toSet

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
        client.pods().inNamespace(ns).withName(pod).delete()
        executorToPod -= id
        shutdownToPod -= id
      } catch {
        case e: Exception => logError(s"Error killing executor pod $pod", e)
          // TODO: we need some retry mechanism depending on the type of failure
      }
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
    val podName = s"$executorBaseName-$executorNum"

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

    val pod = buildPod(labelMap, podName, submitArgs)
    client.pods().inNamespace(ns).withName(podName).create(pod)
    podName
  }

  private def buildPod(labelMap: Map[String, String], podName: String,
                       submitArgs: ArrayBuffer[String]): Pod = {
    val pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(podName)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")
      .addNewContainer()
      .withName("spark-executor")
      .withImage(sparkImage)
      .withImagePullPolicy("IfNotPresent")
      .withCommand("/opt/executor.sh")
      .withArgs(submitArgs: _*)
      .endContainer()

    buildPodUtil(pod)
  }

  private def buildPodUtil(pod: PodFluent.SpecNested[PodBuilder]): Pod = {
    if (imagePullSecret.nonEmpty) {
      pod.addNewImagePullSecret(imagePullSecret).endSpec().build()
    } else {
      pod.endSpec().build()
    }
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

  override def getDriverLogUrls: Option[Map[String, String]] = None
}
