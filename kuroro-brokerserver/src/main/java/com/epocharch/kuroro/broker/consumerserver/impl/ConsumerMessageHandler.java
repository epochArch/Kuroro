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

package com.epocharch.kuroro.broker.consumerserver.impl;

import com.epocharch.kuroro.broker.consumerserver.ACKHandlerType;
import com.epocharch.kuroro.common.inner.consumer.ConsumerMessageType;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.NameCheckUtil;
import com.epocharch.kuroro.common.inner.wrap.WrappedConsumerMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedType;
import com.epocharch.kuroro.common.message.TransferDestination;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.channel.Channel;
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

public class ConsumerMessageHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerMessageHandler.class);

  private static ChannelGroup channelGroup = new DefaultChannelGroup();

  private ConsumerWorkerManager consumerWorkerManager;

  private Consumer consumer;

  private int clientThreadCount;

  private boolean readyClose = Boolean.FALSE;

  public ConsumerMessageHandler(ConsumerWorkerManager consumerWorkerManager) {
    this.consumerWorkerManager = consumerWorkerManager;
  }

  public static ChannelGroup getChannelGroup() {
    return channelGroup;
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    LOG.info(e.getChannel().getRemoteAddress() + " connected!");
    channelGroup.add(e.getChannel());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    removeChannel(e);
    LOG.error("Exception from " + e.getChannel().getRemoteAddress(), e.getCause());
    e.getChannel().close();

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

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    final Channel channel = e.getChannel();
    if (e.getMessage() instanceof WrappedConsumerMessage) {
      WrappedConsumerMessage wrappedConsumerMessage = (WrappedConsumerMessage) e.getMessage();
      if (ConsumerMessageType.HEARTBEAT.equals(wrappedConsumerMessage.getType())) {
        LOG.debug("Heartbeat from {}", channel.getRemoteAddress());
        WrappedMessage writerHeartBeat = null;
        if (consumer.getConsumerOffset() != null) {
          writerHeartBeat = new WrappedMessage(WrappedType.HEARTBEAT,
              consumer.getConsumerOffset().getRestOffset());
        } else {
          writerHeartBeat = WrappedMessage.HEARTBEAT;
        }
        channel.write(writerHeartBeat);
        return;
      } else if (ConsumerMessageType.WAKE.equals(wrappedConsumerMessage.getType())) {
        boolean isValid = false;
        String topic = wrappedConsumerMessage.getDest().getName();
        //对异地zone的topic格式进行特判，有点恶心……
        if (StringUtils.countMatches(topic, TransferDestination.SEPARATOR) == 2) {
          String[] array = StringUtils.split(topic, TransferDestination.SEPARATOR);
          String t = array[0];
          String zone = array[1];
          String consumerid = array[2];
          if ((KuroroZkUtil.allIdcZoneName().contains(zone) || KuroroZkUtil.allIDCName()
              .contains(zone)) && NameCheckUtil
              .isTopicNameValid(t) && NameCheckUtil.isConsumerIdValid(consumerid)) {
            isValid = true;
          }

        } else {
          isValid = NameCheckUtil.isTopicNameValid(topic);
        }

        if (!isValid) {
          LOG.error(topic + " TopicName inValid from " + channel.getRemoteAddress());
          channel.close();
          return;
        }
        clientThreadCount = wrappedConsumerMessage.getThreadCount();
        if (clientThreadCount > consumerWorkerManager.getConsumerConfig()
            .getMaxClientThreadCount()) {
          LOG.warn(channel.getRemoteAddress() + " with " + consumer
              + "clientThreadCount greater than MaxClientThreadCount("
              + consumerWorkerManager.getConsumerConfig().getMaxClientThreadCount() + ")");
          clientThreadCount = consumerWorkerManager.getConsumerConfig().getMaxClientThreadCount();
        }
        String strConsumerId = wrappedConsumerMessage.getConsumerId();

        if (!NameCheckUtil.isConsumerIdValid(wrappedConsumerMessage.getConsumerId())) {
          LOG.error("ConsumerId inValid from " + channel.getRemoteAddress());
          channel.close();
          return;
        }
        consumer = new Consumer(strConsumerId, wrappedConsumerMessage.getDest(),
            wrappedConsumerMessage.getConsumerType(),
            wrappedConsumerMessage.getIdcName(), wrappedConsumerMessage.getZone(),
            wrappedConsumerMessage.getPoolId(),
            wrappedConsumerMessage.getConsumeLocalZoneFilter(),
            wrappedConsumerMessage.getConsumerOffset());
        LOG.info("received greet from " + e.getChannel().getRemoteAddress() + " with " + consumer);
        consumerWorkerManager.handleSpreed(channel, consumer, clientThreadCount,
            wrappedConsumerMessage.getMessageFilter(),
            wrappedConsumerMessage.getConsumeLocalZoneFilter(),
            wrappedConsumerMessage.getConsumerOffset());
      }
      ConsumerMessageType type = wrappedConsumerMessage.getType();
      if (ConsumerMessageType.ACK.equals(type) || ConsumerMessageType.RESEND.equals(type)
          || ConsumerMessageType.TRANSFER_ACK
          .equals(type)) {
        if (wrappedConsumerMessage.getNeedClose() || readyClose) {
          if (!readyClose) {
            Thread thread = consumerWorkerManager.getThreadFactory().newThread(new Runnable() {

              @Override
              public void run() {
                try {
                  Thread.sleep(
                      consumerWorkerManager.getConsumerConfig().getCloseChannelMaxWaitingTime());
                } catch (InterruptedException e) {
                  LOG.error("CloseChannelThread InterruptedException", e);
                }
                LOG.info("CloseChannelMaxWaitingTime reached, close channel " + channel
                    .getRemoteAddress() + " with "
                    + consumer);
                channel.close();
                consumerWorkerManager.handleChannelDisconnect(channel, consumer);
              }
            }, consumer.toString() + "-CloseChannelThread-");
            thread.setDaemon(true);
            thread.start();
          }
          clientThreadCount--;
          readyClose = Boolean.TRUE;
        }
        ACKHandlerType ackHandlerType = ACKHandlerType.SEND_MESSAGE;
        if (readyClose && clientThreadCount == 0) {
          ackHandlerType = ACKHandlerType.CLOSE_CHANNEL;
        } else if (readyClose && clientThreadCount > 0) {
          ackHandlerType = ACKHandlerType.NO_SEND;
        } else if (!readyClose && ConsumerMessageType.RESEND.equals(type)) {
          ackHandlerType = ACKHandlerType.RESEND;
        } else if (!readyClose && ConsumerMessageType.TRANSFER_ACK.equals(type)) {
          ackHandlerType = ACKHandlerType.TRANSFER;
        }
        consumerWorkerManager
            .handleAck(channel, consumer, wrappedConsumerMessage.getMessageId(), ackHandlerType);
      }
    } else {
      LOG.warn("the received message is not wrappedConsumeMessage");
    }
  }

  private void removeChannel(ChannelEvent e) {
    channelGroup.remove(e.getChannel());
    Channel channel = e.getChannel();
    consumerWorkerManager.handleChannelDisconnect(channel, consumer);
  }

  // 生成唯一consumerId
  private String fakeCid() {
    return UUID.randomUUID().toString();
  }
}
