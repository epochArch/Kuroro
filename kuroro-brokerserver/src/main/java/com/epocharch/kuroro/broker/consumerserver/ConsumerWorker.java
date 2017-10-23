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

package com.epocharch.kuroro.broker.consumerserver;

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.channel.Channel;

public interface ConsumerWorker {

  void handleSpreed(Channel channel, int clientThreadCount);

  void handleAck(Channel channel, Long ackedMsgId, ACKHandlerType type);

  void handleChannelDisconnect(Channel channel);

  /**
   * 关闭获取消息的线程
   */
  void closeMessageFetcherThread();

  void closeAckExecutor();

  void close();

  boolean allChannelDisconnected();

  long getMaxAckedMessageId();

  ConsumerType getConsumerType();

  Map<String, AtomicLong> getConsumerIPdequeueCount();

  Map<String, AtomicLong> getConsumerIPdequeueSize();

  ConsumerOffset getOffset();
}
