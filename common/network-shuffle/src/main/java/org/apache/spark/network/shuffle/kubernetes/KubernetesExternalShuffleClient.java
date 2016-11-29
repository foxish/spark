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
package org.apache.spark.network.shuffle.kubernetes;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.shuffle.ExternalShuffleClient;
import org.apache.spark.network.shuffle.protocol.kubernetes.ApplicationComplete;
import org.apache.spark.network.util.TransportConf;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KubernetesExternalShuffleClient extends ExternalShuffleClient {

  public KubernetesExternalShuffleClient(
      TransportConf conf,
      SecretKeyHolder secretKeyHolder,
      boolean saslEnabled,
      boolean saslEncryptionEnabled) {
    super(conf, secretKeyHolder, saslEnabled, saslEncryptionEnabled);
  }

  public void sendApplicationComplete(String host, int port) throws IOException {
    checkInit();
    ByteBuffer applicationComplete = new ApplicationComplete(appId).toByteBuffer();
    TransportClient client = clientFactory.createClient(host, port);
    try {
      client.send(applicationComplete);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
