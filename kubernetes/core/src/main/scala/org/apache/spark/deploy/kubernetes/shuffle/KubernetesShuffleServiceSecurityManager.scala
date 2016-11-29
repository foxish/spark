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

import com.google.common.base.Charsets
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.kubernetes.KubernetesClientBuilder

private[spark] class KubernetesShuffleServiceSecurityManager(
  private val shuffleServiceSecretsNamespace: String,
  private val kubernetesMaster: String,
  private val sparkConf: SparkConf) extends SecurityManager(sparkConf) {

  private val kubernetesClient = KubernetesClientBuilder.buildFromWithinPod(
    kubernetesMaster, shuffleServiceSecretsNamespace)

  def stop(): Unit = {
    try {
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Failed to shut down kubernetes client.", e)
    }
  }

  override def getSecretKey(appId: String): String = {
    new String(Base64.decodeBase64(kubernetesClient.secrets()
      .withName(s"spark-secret-$appId")
      .get()
      .getData
      .asScala
      .getOrElse("spark-shuffle-secret", throw new IllegalArgumentException(
        s"Expected spark-shuffle-secret to be registered for app $appId"))), Charsets.UTF_8)
  }

  def applicationComplete(appId: String): Unit = {
    try {
      kubernetesClient.secrets().withName(secretNameForApp(appId)).delete()
    } catch {
      case e: Throwable => logError(s"Failed to delete secret for app $appId; perhaps auth" +
        s" was not enabled?", e)
    }
  }

  private def secretNameForApp(appId: String) = s"spark-secret-$appId"
}