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

package com.epocharch.kuroro.broker.leaderserver;

import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.SimpleThreadPool;
import com.epocharch.kuroro.common.protocol.json.JsonDecoder;
import com.epocharch.kuroro.common.protocol.json.JsonEncoder;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

public class LeaderClientPipeLineFactory implements ChannelPipelineFactory {

  private static final int READ_TIMEOUT_SECONDS = 60;
  private static final int WRITE_TIMEOUT_SECONDS = 40;
  private static boolean IS_HEARTBEAT = Boolean
      .parseBoolean(System.getProperty("leader.heartbeat", "true"));
  private static Timer timer = new HashedWheelTimer(
      new DefaultThreadFactory("Leader-Client-HashedWheelTimer"));
  private LeaderClient client;
  private SimpleThreadPool threadPool;

  public LeaderClientPipeLineFactory(LeaderClient client, SimpleThreadPool threadPool) {
    this.client = client;
    this.threadPool = threadPool;
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline pipeline = Channels.pipeline();
    if (IS_HEARTBEAT) {
      pipeline.addLast("heartbeat",
          new LeaderHeartBeatHandler(timer, READ_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS, 0,
              client.getTopicName()));
    }
    pipeline
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
    pipeline.addLast("jsonDecoder", new JsonDecoder(MessageIDPair.class));
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
    pipeline.addLast("jsonEncoder", new JsonEncoder(WrapTopicConsumerIndex.class));
    pipeline.addLast("handler", new MessageClientHandler(client, threadPool));
    return pipeline;
  }

}
