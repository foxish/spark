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
package org.apache.spark.deploy.kubernetes.shuffle;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.kubernetes.ApplicationComplete;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class KubernetesShuffleBlockHandler extends ExternalShuffleBlockHandler {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesShuffleBlockHandler.class);

  private final KubernetesShuffleServiceSecurityManager securityManager;

  public KubernetesShuffleBlockHandler(
      TransportConf conf,
      File registeredExecutorFile,
      KubernetesShuffleServiceSecurityManager securityManager) throws IOException {
    super(conf, registeredExecutorFile);
    this.securityManager = securityManager;
  }

  @Override
  protected void handleMessage(
      BlockTransferMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof ApplicationComplete) {
      String appId = ((ApplicationComplete) msgObj).getAppId();
      logger.info("Received application complete message for app: " + appId);
      securityManager.applicationComplete(appId);
    } else {
      super.handleMessage(msgObj, client, callback);
    }
  }
}
