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
package org.apache.spark.deploy.kubernetes.shuffle

import java.util.concurrent.CountDownLatch

import org.apache.commons.lang3.Validate

import org.apache.spark.SparkConf
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{ShutdownHookManager, Utils}

private case class KubernetesShuffleServiceArguments(
  kubernetesMaster: String,
  secretsNamespace: String)

private case class KubernetesShuffleServiceArgumentsBuilder(
  kubernetesMaster: Option[String] = None,
  secretsNamespace: Option[String] = None) {
  def build(): KubernetesShuffleServiceArguments = {
    val resolvedKubernetesMaster = kubernetesMaster.getOrElse(
      throw new IllegalArgumentException("Kubernetes master must be specified."))
    val resolvedSecretsNamespace = secretsNamespace.getOrElse(
      throw new IllegalArgumentException("Secrets namespace must be specified."))
    KubernetesShuffleServiceArguments(resolvedKubernetesMaster, resolvedSecretsNamespace)
  }
}

private object KubernetesShuffleServiceArgumentsBuilder extends SparkSubmitArgumentsParser {
  def fromArgsArray(argStrings: Array[String]): KubernetesShuffleServiceArguments = {
    var currentBuilder = new KubernetesShuffleServiceArgumentsBuilder()
    var args = argStrings.toList
    while (args.nonEmpty) {
      currentBuilder = args match {
        case KUBERNETES_MASTER :: value :: tail =>
          args = tail
          currentBuilder.copy(
            kubernetesMaster = Some(Validate.notBlank(value,
              "Kubernetes master must not be empty.")))
        case "--shuffle-service-secrets-namespace" :: value :: tail =>
          args = tail
          currentBuilder.copy(
            secretsNamespace = Some(Validate.notBlank(value,
              "Secrets namespace must not be empty.")))
        case Nil => currentBuilder
        case _ =>
          // TODO fill in usage message
          throw new IllegalArgumentException("Unsupported parameter")
      }
    }
    currentBuilder.build()
  }
}

private[spark] class KubernetesShuffleService(
    sparkConf: SparkConf,
    securityManager: KubernetesShuffleServiceSecurityManager)
  extends ExternalShuffleService(sparkConf, securityManager) {

  override protected def newShuffleBlockHandler(
      conf: TransportConf): ExternalShuffleBlockHandler = {
    new KubernetesShuffleBlockHandler(conf, null, securityManager)
  }

  override def stop(): Unit = {
    try {
      super.stop()
    } catch {
      case e: Throwable => logError("Error stopping shuffle service.", e)
    }
    securityManager.stop()
  }
}

private[spark] object KubernetesShuffleService extends Logging {
  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    Utils.initDaemon(log)
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.authenticate.secret", "unused")
    val parsedArgs = KubernetesShuffleServiceArgumentsBuilder.fromArgsArray(args)
    val securityManager = new KubernetesShuffleServiceSecurityManager(
      kubernetesMaster = parsedArgs.kubernetesMaster,
      shuffleServiceSecretsNamespace = parsedArgs.secretsNamespace,
      sparkConf = sparkConf)
    val server = new KubernetesShuffleService(sparkConf, securityManager)
    server.start()
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down shuffle service.")
      server.stop()
      barrier.countDown()
    }
    // keep running until the process is terminated
    barrier.await()

  }
}