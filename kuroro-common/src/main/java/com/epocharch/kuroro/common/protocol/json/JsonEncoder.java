/*
 * Copyright 2017 EpochArch.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epocharch.kuroro.common.protocol.json;

import java.nio.charset.Charset;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

@SuppressWarnings("rawtypes")
public class JsonEncoder extends OneToOneEncoder {

  private Class clazz;

  public JsonEncoder(Class clazz) {
    super();
    this.clazz = clazz;
  }

  @Override
  protected Object encode(ChannelHandlerContext ctx, Channel channel, Object message)
      throws Exception {
    if (message.getClass() == clazz) {
      JsonBinder jsonBinder = JsonBinder.getNonEmptyBinder();
      String json = jsonBinder.toJson(message);
      byte[] jsonBytes = json.getBytes(Charset.forName("UTF-8"));
      ChannelBuffer channelBuffer = ChannelBuffers.buffer(jsonBytes.length);
      channelBuffer.writeBytes(jsonBytes);
      return channelBuffer;
    }
    return message;
  }

}
