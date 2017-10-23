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

import com.epocharch.kuroro.common.netty.component.SimpleThreadPool;
import java.io.IOException;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageClientHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MessageClientHandler.class);
  private LeaderClient client;
  private SimpleThreadPool threadPool;

  public MessageClientHandler(LeaderClient client, SimpleThreadPool threadPool) {
    this.client = client;
    this.threadPool = threadPool;
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    super.handleUpstream(ctx, e);

  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    if (e != null) {
      if (e.getMessage() != null) {
        final MessageIDPair messageIDPair = (MessageIDPair) e.getMessage();
        if (messageIDPair != null) {
          if (messageIDPair.getCommand() != null && messageIDPair.getCommand()
              .equals(MessageIDPair.CMD_HEARTBEAT)) {
            LOG.info("Heartbeat received from {}", ctx.getChannel().getRemoteAddress());
            return;
          }
          Runnable task = new Runnable() {
            public void run() {
              client.doMessageIDPair(messageIDPair);
            }
          };
          try {
            this.threadPool.execute(task);
          } catch (Exception ex) {
            String msg = "lead execute fail \r\n";
            LOG.error(msg + ex.getMessage(), ex);
          }
        }
      }
    }

  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    if (e.getCause() instanceof IOException) {
      if (e.getChannel() != null) {
        e.getChannel().close();
      }
      if (client != null) {
        if (client.isConnected()) {
          client.close();
        }
      }
    }
  }

}
