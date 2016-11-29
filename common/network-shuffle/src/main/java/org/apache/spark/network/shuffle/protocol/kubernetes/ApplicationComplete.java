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
package org.apache.spark.network.shuffle.protocol.kubernetes;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

import java.util.Objects;

public class ApplicationComplete extends BlockTransferMessage {

  private final String appId;

  public ApplicationComplete(String appId) {
    this.appId = appId;
  }

  @Override
  protected Type type() {
    return Type.APPLICATION_COMPLETE;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ApplicationComplete)) {
      return false;
    }
    return Objects.equals(appId, ((ApplicationComplete) other).appId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId);
  }

  public String getAppId() {
    return appId;
  }

  public static ApplicationComplete decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    return new ApplicationComplete(appId);
  }
}
