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

import java.io.File
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.api.model.{PodBuilder, ServiceBuilder}
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder
import io.fabric8.kubernetes.client.dsl.LogWatch
import org.apache.spark.deploy.Command
import org.apache.spark.deploy.kubernetes.ClientArguments
import org.apache.spark.{io, _}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

import collection.JavaConverters._
import org.apache.spark.util.Utils

import scala.util.Random

private[spark] object KubernetesClusterScheduler {
  def defaultNameSpace = "default"
  def defaultServiceAccountName = "default"
  def defaultCores = 1
  def defaultMemory = "1g"
}

/**
  * This is a simple extension to ClusterScheduler
  * */
private[spark] class KubernetesClusterScheduler(conf: SparkConf)
    extends Logging {
  logInfo("Creating KubernetesClusterScheduler instance")

  var client = setupKubernetesClient()
  val driverName = s"spark-driver-${Random.alphanumeric take 5 mkString("")}".toLowerCase()
  val svcName = s"spark-svc-${Random.alphanumeric take 5 mkString("")}".toLowerCase()
  val nameSpace = conf.get(
    "spark.kubernetes.namespace",
    KubernetesClusterScheduler.defaultNameSpace)
  val serviceAccountName = conf.get(
    "spark.kubernetes.serviceAccountName",
    KubernetesClusterScheduler.defaultServiceAccountName)
  val driverCores = conf.getInt(
    "spark.driver.cores",
     KubernetesClusterScheduler.defaultCores)
  val driverMemory = conf.getSizeAsMb(
    "spark.driver.memory",
    KubernetesClusterScheduler.defaultMemory)

  // Anything that should either not be passed to driver config in the cluster, or
  // that is going to be explicitly managed as command argument to the driver pod
  val confBlackList = scala.collection.Set(
    "spark.master",
    "spark.app.name",
    "spark.submit.deployMode",
    "spark.executor.jar",
    "spark.dynamicAllocation.enabled",
    "spark.shuffle.service.enabled")

  def start(args: ClientArguments): Unit = {
    startDriver(client, args)
  }

  def stop(): Unit = {
    client.pods().inNamespace(nameSpace).withName(driverName).delete()
    client
      .services()
      .inNamespace(nameSpace)
      .withName(svcName)
      .delete()
  }

  def startDriver(client: KubernetesClient,
                  args: ClientArguments): Unit = {
    logInfo("Starting spark driver on kubernetes cluster")

    // image needs to support shim scripts "/opt/driver.sh" and "/opt/executor.sh"
    val sparkImage = conf.getOption("spark.kubernetes.sparkImage").getOrElse {
      // TODO: this needs to default to some standard Apache Spark image
      throw new SparkException("Spark image not set. Please configure spark.kubernetes.sparkImage")
    }

    // This is the URL of the client jar.
    val clientJarUri = args.userJar

    // This is the kubernetes master we're launching on.
    val kubernetesHost = "k8s://" + client.getMasterUrl().getHost()
    logInfo("Using as kubernetes-master: " + kubernetesHost.toString())

    val submitArgs = scala.collection.mutable.ArrayBuffer.empty[String]
    submitArgs ++= Vector(
      clientJarUri,
      s"--class=${args.userClass}",
      s"--master=$kubernetesHost",
      s"--conf spark.executor.jar=$clientJarUri")

    submitArgs ++= conf.getAll.filter { case (name, _) => !confBlackList.contains(name) }
      .map { case (name, value) => s"--conf ${name}=${value}" }

    if (conf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      submitArgs ++= Vector(
        "--conf spark.dynamicAllocation.enabled=true",
        "--conf spark.shuffle.service.enabled=true")
    }

    // these have to come at end of arg list
    submitArgs ++= Vector("/opt/spark/kubernetes/client.jar",
      args.userArgs.mkString(" "))

    val reqs = new ResourceRequirementsBuilder()
      .withRequests(Map(
        "cpu" -> new Quantity(s"$driverCores"),
        "memory" -> new Quantity(s"${driverMemory}Mi")
        ).asJava)
      .build()

    val labelMap = Map("type" -> "spark-driver")
    val pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(driverName)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")
      .withServiceAccount(serviceAccountName)
      .addNewContainer()
      .withName("spark-driver")
      .withImage(sparkImage)
      .withImagePullPolicy("Always")
      .withResources(reqs)
      .withCommand(s"/opt/driver.sh")
      .withArgs(submitArgs :_*)
      .endContainer()
      .endSpec()
      .build()
    client.pods().inNamespace(nameSpace).withName(driverName).create(pod)

    var svc = new ServiceBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(svcName)
      .endMetadata()
      .withNewSpec()
      .addNewPort()
      .withPort(4040)
      .withNewTargetPort()
      .withIntVal(4040)
      .endTargetPort()
      .endPort()
      .withSelector(labelMap.asJava)
      .withType("LoadBalancer")
      .endSpec()
      .build()

    client
      .services()
      .inNamespace(nameSpace)
      .withName(svcName)
      .create(svc)

//    try {
//      while (true) {
//        client
//          .pods()
//          .inNamespace("default")
//          .withName("spark-driver")
//          .tailingLines(10)
//          .watchLog(System.out)
//        Thread.sleep(5 * 1000)
//      }
//    } catch {
//      case e: Exception => logError(e.getMessage)
//    }
  }

  def setupKubernetesClient(): KubernetesClient = {
    val sparkHost = new java.net.URI(conf.get("spark.master")).getHost()

    var config = new ConfigBuilder().withNamespace(nameSpace)
    if (sparkHost != "default") {
      config = config.withMasterUrl(sparkHost)
    }

    // TODO: support k8s user and password options:
    // .withTrustCerts(true)
    // .withUsername("admin")
    // .withPassword("admin")

    new DefaultKubernetesClient(config.build())
  }
}
