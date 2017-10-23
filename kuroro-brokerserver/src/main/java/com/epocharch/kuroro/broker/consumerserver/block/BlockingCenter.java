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
import com.epocharch.kuroro.common.message.Message;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 创建线程安全的topic-Queue
 */
public class BlockingCenter {

  private static int thresholdOfQueue = Integer
		  .parseInt(System.getProperty("global.block.queue.threshold", "50"));// 阀值

  static {
    if (thresholdOfQueue <= 0) {
      thresholdOfQueue = 50;
    }
  }

  private final int LOCK_NUM_FOR_CREATE_TOPIC_BLOCK = 10;
  private final ConcurrentHashMap<String, TopicBlock> topicBlocks = new ConcurrentHashMap<String, TopicBlock>();
  private ReentrantLock[] locksForCreateTopicBlock = new ReentrantLock[LOCK_NUM_FOR_CREATE_TOPIC_BLOCK];
  private ConcurrentHashMap<String, WeakReference<MessageBlockingQueue>> messageQueues = new ConcurrentHashMap<String, WeakReference<MessageBlockingQueue>>();
  private MessageRefetch messageRefetch;
  private int capacityOfQueue = Integer.MAX_VALUE;
  private int delayBase = Integer
      .parseInt(System.getProperty("global.block.queue.delay.base", "250"));
  private int delayUpperbound = Integer
      .parseInt(System.getProperty("global.block.queue.delay.upper.bound", "1000"));

  {
    for (int i = 0; i < locksForCreateTopicBlock.length; i++) {
      locksForCreateTopicBlock[i] = new ReentrantLock();
    }
  }

  protected TopicBlock getTopicBlock(String topicName) {
    TopicBlock topicBlock = topicBlocks.get(topicName);
	  if (topicBlock != null) {
		  return topicBlock;
	  }
    // topicBuffer不存在，须创建
    ReentrantLock reentrantLock = locksForCreateTopicBlock[index(topicName)];
    try {
      reentrantLock.lock();
      topicBlock = topicBlocks.get(topicName);
      if (topicBlock == null) {
        topicBlock = new TopicBlock(topicName);
        topicBlocks.put(topicName, topicBlock);
      }
    } finally {
      reentrantLock.unlock();
    }
    return topicBlock;

  }

  public BlockingQueue<Message> getMessageQueue(String topicName, String consumerId) {

    return this.getTopicBlock(topicName).getMessageQueue(consumerId);
  }

  public CloseBlockingQueue<Message> createMessageQueue(String topicName, String consumerId,
      MessageFilter messageFilter,
      boolean needCompensation, MessageFilter consumeLocalZoneFilter, String zone,
      ConsumerOffset offset) {
    return this.getTopicBlock(topicName)
        .createMessageQueue(consumerId, messageFilter, needCompensation, consumeLocalZoneFilter,
            zone, offset);
  }

  private int index(String topicName) {
    int hashcode = topicName.hashCode();
    hashcode = hashcode == Integer.MIN_VALUE ? 0 : Math.abs(hashcode);// 保证非负
    return hashcode % LOCK_NUM_FOR_CREATE_TOPIC_BLOCK;
  }

  public void setCapacity(int capacity) {
    this.capacityOfQueue = capacity;
  }

  public void setThreshold(int threshold) {
    this.thresholdOfQueue = threshold;
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

  private class TopicBlock {

    private final String topicName;

    public TopicBlock(String topicName) {
      this.topicName = topicName;
    }

    public BlockingQueue<Message> getMessageQueue(String consumerId) {
      if (consumerId == null) {
        throw new IllegalArgumentException("consumerId is null!");
      }
      Reference<MessageBlockingQueue> ref = messageQueues.get(consumerId);
      if (ref == null) {
        return null;
      }
      return ref.get();
    }

    public BlockingQueue<Message> createMessageQueue(String cid, boolean needCompensation) {
      return this.createMessageQueue(cid, // tailMessageId,
          null, needCompensation, null, null, null);
    }

    public CloseBlockingQueue<Message> createMessageQueue(String consumerId,
        MessageFilter messageFilter, boolean needCompensation,
        MessageFilter consumeLocalZoneFilter, String zone, ConsumerOffset offset) {
      if (consumerId == null) {
        throw new IllegalArgumentException("consumerId is null.");
      }

      MessageBlockingQueue messageBlockingQueue = new MessageBlockingQueue(consumerId,
          this.topicName, thresholdOfQueue,
          capacityOfQueue, messageFilter, needCompensation, consumeLocalZoneFilter, zone, offset);

      if (delayBase != -1) {
        messageBlockingQueue.setDelayBase(delayBase);
      }
      if (delayUpperbound != -1) {
        messageBlockingQueue.setDelayUpperbound(delayUpperbound);
      }
      messageBlockingQueue.setMessageRefetch(messageRefetch);
      messageQueues.put(consumerId, new WeakReference<MessageBlockingQueue>(messageBlockingQueue));
      return messageBlockingQueue;
    }

  }
}
