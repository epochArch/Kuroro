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

package com.epocharch.kuroro.broker.consumerserver.block;

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.message.Message;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MessageBlockingQueue extends LinkedBlockingQueue<Message> implements
    CloseBlockingQueue<Message> {

  private static final long serialVersionUID = -8400494292464878520L;
  private final static Logger LOG = LoggerFactory.getLogger(MessageBlockingQueue.class);
  private final String consumerId;
  private final String topicName;
  private final transient MessageRefetchThread messageRefetchThread;
  /**
   * 最小剩余数量,当queue的消息数量小于threshold时，会触发从数据库加载数据的操作
   */
  private final int threshold;
  public AtomicLong fetchTimes = new AtomicLong();
  public AtomicLong fetchSize = new AtomicLong();
  protected transient MessageRefetch messageRefetch;
  protected volatile Long tailMessageId;
  protected MessageFilter messageFilter;
  private AtomicBoolean isClosed = new AtomicBoolean(false);
  private int delayBase = 100;
  private int delayUpperbound = 500;
  private ReentrantLock reentrantLock = new ReentrantLock(true);
  private transient Condition condition = reentrantLock.newCondition();
  private boolean needCompensation = false;
  private MessageFilter consumeLocalZoneFilter;
  private String zone;
  private ConsumerOffset offset;

  public MessageBlockingQueue(String consumerId, String topicName, int threshold, int capacity,
      boolean needCompensation) {
    super(capacity);
    this.needCompensation = needCompensation;
    this.consumerId = consumerId;
    this.topicName = topicName;
    if (threshold <= 0) {
      throw new IllegalArgumentException("threshold: " + threshold);
    }
    this.threshold = threshold;
    messageRefetchThread = new MessageRefetchThread();
    messageRefetchThread.start();
  }

  public MessageBlockingQueue(String consumerId, String topicName, int threshold, int capacity,
      MessageFilter messageFilter,
      boolean needCompensation, MessageFilter consumerLocalZoneFilter, String zone,
      ConsumerOffset offset) {
    super(capacity);
    this.needCompensation = needCompensation;
    this.consumerId = consumerId;
    this.topicName = topicName;
    if (threshold < 0) {
      throw new IllegalArgumentException("threshold: " + threshold);
    }
    this.threshold = threshold;
    this.messageFilter = messageFilter;
    this.zone = zone;
    this.consumeLocalZoneFilter = consumerLocalZoneFilter;
    this.offset = offset;
    messageRefetchThread = new MessageRefetchThread();
    messageRefetchThread.start();
  }

  @Override
  public Message take() throws InterruptedException {
    // 但是极端情况下，可能出现：
    // 后台线程某时刻判断size()是足够的，所以wait；但在wait前，queue的元素又被取光了，外部调用者继续在take()上阻塞；而此时后台线程也wait，就‘死锁’了。
    // 即，size()的判断和fetch消息的操作，比较作为同步块原子化。才能从根本上保证线程安全。
    throw new UnsupportedOperationException(
        "Don't call this operation, call 'poll(long timeout, TimeUnit unit)' instead.");
  }

  @Override
  public Message poll() {
    // 如果剩余元素数量小于最低限制值threshold，就启动一个“获取DB数据的后台线程”去DB获取数据，并添加到Queue的尾部
    if (super.size() < threshold) {
      ensureLeftMessage();
    }
    return super.poll();
  }

  @Override
  public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
    // 如果剩余元素数量小于最低限制值threshold，就启动一个“获取DB数据的后台线程”去DB获取数据，并添加到Queue的尾部
    if (super.size() < threshold) {
      ensureLeftMessage();
    }
    return super.poll(timeout, unit);
  }

  /**
   * 唤醒获取DB数据的后台线程去DB获取数据，并添加到Queue的尾部
   */
  private void ensureLeftMessage() {
    if (messageRefetch == null) {
      return;
    }
    // 只有一个线程能唤醒“获取DB数据的后台线程”
    if (reentrantLock.tryLock()) {
      condition.signal();
      reentrantLock.unlock();
    }
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      this.messageRefetchThread.interrupt();
    }
  }

  @Override
  public void setCompensation(boolean needCompensation) {
    this.needCompensation = needCompensation;
  }

  @Override
  public void isClosed() {
    if (isClosed.get()) {
      throw new RuntimeException("MessageBlockingQueue- already closed! ");
    }
  }

  public void setDelayBase(int delayBase) {
    this.delayBase = delayBase;
  }

  public void setDelayUpperbound(int delayUpperbound) {
    this.delayUpperbound = delayUpperbound;
  }

  public void setMessageRefetch(MessageRefetch messageRefetch) {
    this.messageRefetch = messageRefetch;
  }

  private class MessageRefetchThread extends Thread {

    private DefaultPullStrategy pullStrategy = new DefaultPullStrategy(delayBase, delayUpperbound);

    public MessageRefetchThread() {

      String threadName = (consumeLocalZoneFilter == null ?
          "kuroro-messagerefetch-(topic=" + topicName + ",customerid=" + consumerId + ")" :
          "kuroro-messagerefetch-(topic=" + topicName + ",customerid=" + consumerId + ",zone="
              + zone + ")");

      this.setName(threadName);
      this.setDaemon(true);
    }

    @Override
    public void run() {
      LOG.info("thread start:" + this.getName());
      while (!this.isInterrupted()) {
        reentrantLock.lock();
        try {
          condition.await();
          fetchTimes.incrementAndGet();
          try {
            refetchMessage();
          } catch (Throwable t) {
            LOG.error("MessageBlockingQueue refetch error of " + topicName + ":" + consumerId, t);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          reentrantLock.unlock();
        }
      }
      LOG.info("thread done:" + this.getName());
    }

    @SuppressWarnings("rawtypes")
    private void refetchMessage() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("retriveMessage() start:" + this.getName());
      }
      try {
        List messages = messageRefetch.refetchMessage(MessageBlockingQueue.this.topicName,
            MessageBlockingQueue.this.consumerId,
            MessageBlockingQueue.this.messageFilter, MessageBlockingQueue.this.needCompensation,
            consumeLocalZoneFilter, zone,
            offset);
        if (messages != null && messages.size() > 0) {
          fetchSize.addAndGet(messages.size());
          for (int i = 0; i < messages.size(); i++) {
            Message message = (Message) messages.get(i);
            try {
              MessageBlockingQueue.this.put(message);
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "add message to (topic=" + topicName + ",consumeId=" + consumerId + ") queue:"
                        + message
                        .toString());
              }
            } catch (InterruptedException e) {
              this.interrupt();
              break;
            }
          }
          // 如果本次获取完，queue的消息条数仍然比最低阀值小，那么消费者与生产者很有可能速度差不多，此时为了
          // 避免retrieve线程不断被唤醒，适当地睡眠一段时间
          if (MessageBlockingQueue.this.size() < MessageBlockingQueue.this.threshold) {
            pullStrategy.fail(true);
          } else {
            pullStrategy.succeess();
          }
        }
      } catch (RuntimeException e1) {
        LOG.error(e1.getMessage(), e1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("retriveMessage() done:" + this.getName());
      }

    }
  }

}
