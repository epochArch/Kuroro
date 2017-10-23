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

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderMessageHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderMessageHandler.class);
  private static ChannelGroup channelGroup = new DefaultChannelGroup();
  private LeaderWorkerManager leaderWorkerManager;

  public LeaderMessageHandler(LeaderWorkerManager leaderWorkerManager) {
    this.leaderWorkerManager = leaderWorkerManager;
  }

  public static ChannelGroup getChannelGroup() {
    return channelGroup;
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) {

    if (e instanceof ChannelStateEvent) {
      LOG.info(e.toString());
    }
    try {
      super.handleUpstream(ctx, e);
    } catch (Exception e1) {
      LOG.warn("Handle Upstrem Exceptions." + e1);
    }
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    if (e.getMessage() != null) {
      WrapTopicConsumerIndex topicConsumer = (WrapTopicConsumerIndex) e.getMessage();
      if (topicConsumer.getCommand() != null && topicConsumer.getCommand()
          .equals(WrapTopicConsumerIndex.CMD_HEARTBEAT)) {
        LOG.info("Heartbeat received from {}", ctx.getChannel().getRemoteAddress());
        ctx.getChannel().write(MessageIDPair.HEARTBEAT);
        return;
      }
      topicConsumer.setRequestTime(System.currentTimeMillis());
      if (topicConsumer != null) {
        String topicConsumerIndex = topicConsumer.getTopicConsumerIndex();
        boolean compensate = false;
        if (topicConsumer.getCommand() != null) {
          if (topicConsumer.getCommand().equals("stop")) {
            leaderWorkerManager.closeLeaderWorkByTopic(topicConsumer.getTopicName());
            confirmWorkerStoppedForChannel(topicConsumerIndex, topicConsumer.getSquence(), e);
            return;
          } else if (WrapTopicConsumerIndex.CMD_COMPENSATION.equals(topicConsumer.getCommand())) {
            compensate = true;
          }
        }
        LeaderWorker leaderWork = null;
        try {
          leaderWork = leaderWorkerManager.findOrCreateLeaderWork(topicConsumer, compensate);
        } catch (Exception e2) {
          LOG.error(e2.getMessage(), e2);
        }
        if (leaderWork != null) {

          try {
            leaderWork.putMessage(e);
          } catch (Exception e1) {
            LOG.error(e1.getMessage(), e1.getCause());
          }
        } else {
          LOG.warn(topicConsumerIndex + ":leaderworker is null");
          sendEmptyDataForChannel(topicConsumerIndex, topicConsumer.getSquence(), e);
        }
      }
    }

  }

  private void sendEmptyDataForChannel(String topicConsumerIndex, Long squence, MessageEvent e) {
    MessageIDPair pair = new MessageIDPair();
    pair.setTopicConsumerIndex(topicConsumerIndex);
    pair.setMinMessageId(null);
    pair.setMaxMessageId(null);
    pair.setSequence(squence);
    e.getChannel().write(pair);
  }

  private void confirmWorkerStoppedForChannel(String topicConsumerIndex, Long squence,
      MessageEvent e) {
    MessageIDPair pair = new MessageIDPair();
    pair.setTopicConsumerIndex(topicConsumerIndex);
    pair.setMinMessageId(null);
    pair.setMaxMessageId(null);
    pair.setSequence(squence);
    pair.setCommand("stopped");
    e.getChannel().write(pair);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    removeChannel(e);
    LOG.error("Exception from " + e.getChannel().getRemoteAddress(), e.getCause());
    e.getChannel().close();
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    LOG.info(e.getChannel().getRemoteAddress() + " connected!");
    channelGroup.add(e.getChannel());
  }

  @Override
  public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    removeChannel(e);
    super.channelDisconnected(ctx, e);
    LOG.info(e.getChannel().getRemoteAddress() + " disconnected!");
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    removeChannel(e);
    super.channelClosed(ctx, e);
    LOG.info(e.getChannel().getRemoteAddress() + " closed!");
  }

  private void removeChannel(ChannelEvent e) {
    channelGroup.remove(e.getChannel());
  }
}
