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

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderWorkerImpl implements LeaderWorker {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderWorkerImpl.class);
  private final WrapTopicConsumerIndex topicConsumerIndex;
  private final int fetchSize = Integer
      .parseInt(System.getProperty("global.block.queue.fetchsize", "200"));
  private final Long DEFAULT_TIMEOUT = Long
      .parseLong(System.getProperty("leader.request.timeout", "10000"));
  private final ExecutorService executor;
  private final int leaderQueueSize = Integer
      .parseInt(System.getProperty("leader.queue.size", "100"));
  private final LinkedBlockingDeque<MessageEvent> queue = new LinkedBlockingDeque<MessageEvent>(
      leaderQueueSize);
  private final Compensator compensator;
  private final String topic;
  private final String consumer;
  private final AtomicBoolean isCompensating = new AtomicBoolean(false);
  private final String zone;
  private final int index = 0;
  private volatile Long currentMessageId = null;
  private MessageDAO messageDAO;
  private AckDAO ackDAO;
  private AtomicBoolean isClosed = new AtomicBoolean(false);
  private volatile ConsumerOffset offset;
  private volatile Long requestLeaderTime;

  public LeaderWorkerImpl(WrapTopicConsumerIndex topicConsumerIndex,
      LeaderWorkerManager leaderWorkerManager) {
    this.topicConsumerIndex = topicConsumerIndex;
    this.topic = topicConsumerIndex.getTopicName();
    this.consumer = topicConsumerIndex.getConsumerId();
    this.zone = topicConsumerIndex.getZone();
    this.messageDAO = leaderWorkerManager.getMessageDAO();
    this.ackDAO = leaderWorkerManager.getAckDAO();
    this.compensator = leaderWorkerManager.getCompensator();
    this.offset = topicConsumerIndex.getOffset();
    String threadName = (zone == null ? ("Leading-" + topic + ":" + consumer)
        : ("Leading-" + topic + ":" + consumer + ":" + zone));
    this.executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory(threadName));
    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (!isClosed()) {
          try {
            MessageEvent e = queue.take();
            LOG.debug("Leader client request received {}", e.getMessage());
            refetchMessageId(e);
          } catch (InterruptedException e) {
            close();
            LOG.error("InterruptedException in thread " + Thread.currentThread().getName(), e);
            Thread.currentThread().interrupt();
          } catch (Throwable t) {
            close();
            LOG.error("Leading thread error: " + Thread.currentThread().getName(), t);
          }
        }
      }
    });
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      this.stopCompensation();
      this.currentMessageId = null;
      this.queue.clear();
      if (!this.executor.isShutdown()) {
        this.executor.shutdownNow();
      }
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public String getTopicConsumerIndex() {
    return topicConsumerIndex.getTopicConsumerIndex();
  }

  @Override
  public Long getCurrentMessageId() {
    return currentMessageId;
  }

  @Override
  public void setCurrentMessageId(Long currentMessageId) {
    this.currentMessageId = currentMessageId;
  }

  @Override
  public Long getRequestLeaderTime() {
    return requestLeaderTime;
  }

  @Override
  public void setRequestLeaderTime(Long requestTime) {
    this.requestLeaderTime = requestTime;
  }

  /**
   * 开始补偿
   */
  @Override
  public synchronized void startCompensation() {
    if (this.compensator != null && !compensator.isCompensating(topic, consumer, zone)) {
      if (this.isCompensating.compareAndSet(false, true)) {
        compensator.compensate(topic, consumer, zone);
      }
    } else {
      LOG.error("Start compensation error[topic={},consumer={}, zone={}] " + zone, topic, consumer);
    }
    if (!compensator.isCompensating(topic, consumer, zone)) {
      LOG.error("Start compensation failed[topic={},consumer={}, zone={}] " + zone, topic,
          consumer);
    }
  }

  @Override
  public synchronized void stopCompensation() {
    if (this.compensator != null && compensator.isCompensating(topic, consumer, zone)) {
      if (this.isCompensating.compareAndSet(true, false)) {
        compensator.stopCompensation(topic, consumer, zone);
      }
    } else {
      LOG.error("Stop compensation not necessary[topic={},consumer={}]", topic, consumer);
    }
  }

  @Override
  public String toString() {
    return "->topicConusmerIndex:" + topicConsumerIndex + ";currentMessageId:" + currentMessageId
        + ";state:" + isClosed.get();
  }

  public void putMessage(MessageEvent event) {
    if (event != null) {
      boolean isPut = queue.offer(event);
      if (!isPut) {
        LOG.error("Leader request queue is full: " + topic + "-" + consumer + "-" + zone);
        WrapTopicConsumerIndex topicConsumer = (WrapTopicConsumerIndex) event.getMessage();
        sendEmpty(event, topicConsumer);
      }
    }
  }

  private void sendEmpty(MessageEvent event, WrapTopicConsumerIndex topicConsumer) {
    MessageIDPair pair = new MessageIDPair();
    pair.setTopicConsumerIndex(topicConsumerIndex.getTopicConsumerIndex());
    pair.setMinMessageId(null);
    pair.setMaxMessageId(null);
    pair.setSequence(topicConsumer.getSquence());
    event.getChannel().write(pair);
  }

  private void refetchMessageId(MessageEvent msgEvent) {
    try {
      if (msgEvent != null) {
        final CountDownLatch latch = new CountDownLatch(1);
        WrapTopicConsumerIndex topicConsumer = (WrapTopicConsumerIndex) msgEvent.getMessage();
        Long requestTime = topicConsumer.getRequestTime();
        String topicConsumerIndex = topicConsumer.getTopicConsumerIndex();

        if (requestTime + DEFAULT_TIMEOUT < System.currentTimeMillis()) {
          LOG.warn(topicConsumer.getTopicConsumerIndex() + " time out>" + DEFAULT_TIMEOUT);
        } else {
          // 从补偿表里取
          if (this.isCompensating.get() && !compensator.isInRound(topic, consumer, zone)) {
            final List<Long> toCompensate = this.compensator.getCompensated(topic, consumer, zone);

            if (toCompensate != null && !toCompensate.isEmpty()) {// 有需要补偿的
              Long min = toCompensate.get(0);
              Long max = toCompensate.get(toCompensate.size() - 1);

              MessageIDPair pair = new MessageIDPair();
              pair.setTopicConsumerIndex(topicConsumerIndex);
              pair.setMinMessageId(min);
              pair.setMaxMessageId(max);
              pair.setSequence(topicConsumer.getSquence());
              pair.setCommand(MessageIDPair.COMPENSATION);
              try {
                msgEvent.getChannel().write(pair).addListener(new ChannelFutureListener() {

                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                      compensator.compensated(topic, consumer, toCompensate, zone);// 从待补偿列表删掉
                    }
                    latch.countDown();
                  }
                });
                latch.await();
                LOG.info("Get a compensated id list of topic {} and consumer {}:" + pair.toString(),
                    topic, consumer);
                return;
              } catch (Exception ex) {
                LOG.error(ex.getMessage());
              }
            }
          }

          if (currentMessageId == null) {
            resetConsumerOffset();
            if (currentMessageId == null) {
              currentMessageId = ackDAO.getMaxMessageID(topic, consumer, index, zone);
            }

            if (currentMessageId == null) {
              currentMessageId = messageDAO.getMaxMessageId(topic, index);
              if (currentMessageId == null) {
                currentMessageId = MongoUtil.getLongByCurTime();
                // ackDAO
                ackDAO.add(topic, consumer, currentMessageId,
                    msgEvent.getChannel().getRemoteAddress().toString(), index,
                    zone);
              }
            }
          }

          if (currentMessageId != null) {
            final Long maxMessageId = messageDAO
                .getMessageIDGreaterThan(topic, currentMessageId, fetchSize, index);
            MessageIDPair pair = new MessageIDPair();
            pair.setTopicConsumerIndex(topicConsumerIndex);
            pair.setMinMessageId(currentMessageId);
            pair.setMaxMessageId(maxMessageId);
            pair.setSequence(topicConsumer.getSquence());
            if (offset != null) {
              pair.setRestOffset(offset.getRestOffset());
            }
            try {
              msgEvent.getChannel().write(pair).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  if (future.isSuccess()) {
                    if (maxMessageId != null) {
                      currentMessageId = maxMessageId;
                    }
                  }
                  latch.countDown();
                }
              });
              latch.await();
            } catch (Exception ex) {
              LOG.error(ex.getMessage(), ex);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("refetchMessageId error!", e);
    }
  }

  //message backTracking
  private void resetConsumerOffset() {
    if (offset != null && !offset.getRestOffset()) {
      long offsetValue;
      if (offset.getType().equals(ConsumerOffset.OffsetType.CUSTOMIZE_OFFSET)) {
        offsetValue = offset.getOffsetValue() / 1000;

        if (offsetValue > 0) {
          BSONTimestamp timestamp = new BSONTimestamp((int) offsetValue, 0);
          Long offset = MongoUtil.BSONTimestampToLong(timestamp);
          Long messageId = messageDAO.getMessageIDGreaterThan(topic, offset, fetchSize, index);
          if (messageId != null && messageId > 0) {
            currentMessageId = offset;
          } else {
            LOG.warn(
                "====customize offset is not existed!=====" + topic + ", offset: " + offsetValue);
          }
        }
      } else if (offset.getType().equals(ConsumerOffset.OffsetType.LARGEST_OFFSET)) {
        currentMessageId = messageDAO.getMaxMessageId(topic, index);
      } else if (offset.getType().equals(ConsumerOffset.OffsetType.SMALLEST_OFFSET)) {
        currentMessageId = messageDAO.getMinMessageId(topic, index);
      }

      if (currentMessageId != null) {
        offset.setRestOffset(true);
        LOG.warn("reset consumer offset =====" + currentMessageId);
      }
    }
  }
}
