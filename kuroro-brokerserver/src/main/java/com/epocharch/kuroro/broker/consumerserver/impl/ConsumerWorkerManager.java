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

import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.broker.Config;
import com.epocharch.kuroro.broker.consumerserver.ACKHandlerType;
import com.epocharch.kuroro.broker.consumerserver.ConsumerWorker;
import com.epocharch.kuroro.broker.consumerserver.block.BlockingCenter;
import com.epocharch.kuroro.broker.utils.MessageCountUtil;
import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.ProxyUtil;
import com.epocharch.kuroro.common.jmx.support.JmxSpringUtil;
import com.epocharch.kuroro.common.message.MessageLog;
import com.epocharch.kuroro.common.message.OutMessagePair;
import com.epocharch.kuroro.monitor.intelligent.impl.AnalystService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWorkerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerWorkerManager.class);
  private AckDAO ackDAO;
  private MessageDAO messageDAO;
  private BlockingCenter blockingCenter;
  private Config consumerConfig = Config.getInstance();
  private KuroroThreadFactory threadFactory = new KuroroThreadFactory();
  private Map<Consumer, ConsumerWorker> consumerToConsumerWorker = new ConcurrentHashMap<Consumer, ConsumerWorker>();
  private Thread idleWorkerManagerCheckerThread;
  private Thread maxAckedMessageIdUpdaterThread;
  private JmxSpringUtil jmxSpringUtil;
  private MessageCountUtil messageCountUtil;
  private Map<Consumer, Long> consumerToMaxSaveAckedMessageId = new ConcurrentHashMap<Consumer, Long>();
  private volatile boolean closed = false;
  private AnalystService analystService;

  public void handleAck(Channel channel, Consumer consumer, Long ackMsgId,
      ACKHandlerType ackHandlerType) {
    ConsumerWorker worker = consumerToConsumerWorker.get(consumer);
    if (worker != null) {
      worker.handleAck(channel, ackMsgId, ackHandlerType);
    } else {
      LOG.warn(consumer + " worker don't exits");
      channel.close();
    }
  }

  public void handleSpreed(Channel channel, Consumer consumer, int clientThreadCount,
      MessageFilter messageFilter,
      MessageFilter consumeLocalZoneFilter, ConsumerOffset offset) {
    findOrCreateConsumerWorker(consumer, messageFilter, consumeLocalZoneFilter, offset)
        .handleSpreed(channel, clientThreadCount);
  }

  public void handleChannelDisconnect(Channel channel, Consumer consumer) {
    try {
      if (consumerToConsumerWorker != null) {
        ConsumerWorker worker = consumerToConsumerWorker.get(consumer);
        if (worker != null) {
          worker.handleChannelDisconnect(channel);
        }
      }
    } catch (Exception e) {
    }
  }

  public void close() {
    for (Map.Entry<Consumer, ConsumerWorker> entry : consumerToConsumerWorker.entrySet()) {
      entry.getValue().closeMessageFetcherThread();
    }
    try {
      Thread.sleep(consumerConfig.getWaitAckTimeWhenCloseKuroro());
    } catch (InterruptedException e) {
      LOG.error("close Swc thread InterruptedException", e);
    }
    for (Map.Entry<Consumer, ConsumerWorker> entry : consumerToConsumerWorker.entrySet()) {
      entry.getValue().closeAckExecutor();
    }
    for (Map.Entry<Consumer, ConsumerWorker> entry : consumerToConsumerWorker.entrySet()) {
      entry.getValue().close();
    }
    closed = true;
    if (idleWorkerManagerCheckerThread != null) {
      try {
        idleWorkerManagerCheckerThread.join();
        consumerToConsumerWorker = new ConcurrentHashMap<Consumer, ConsumerWorker>();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (maxAckedMessageIdUpdaterThread != null) {
      try {
        maxAckedMessageIdUpdaterThread.join();
        consumerToMaxSaveAckedMessageId = new ConcurrentHashMap<Consumer, Long>();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    this.messageCountUtil.close();

  }

  public ConsumerWorker findOrCreateConsumerWorker(Consumer consumer, MessageFilter messageFilter,
      MessageFilter consumeLocalZoneFilter,
      ConsumerOffset offset) {
    ConsumerWorker worker = consumerToConsumerWorker.get(consumer);
    if (worker == null) {
      synchronized (consumerToConsumerWorker) {
        if ((worker = consumerToConsumerWorker.get(consumer)) == null) {
          worker = new ConsumerWorkerImpl(consumer, this, messageFilter, consumeLocalZoneFilter,
              offset);
          consumerToConsumerWorker.put(consumer, worker);
        }
      }
    } else if (worker.getOffset() != null) {
      if (worker.getOffset().getRestOffset().booleanValue() == true) {
        consumer.getConsumerOffset().setRestOffset(true);
      }
    }
    return worker;
  }

  public void init() {
    startIdleWorkerCheckerThread();
    startMaxAckedMessageIdUpdaterThread();
    monitorRegister();
  }

  private void monitorRegister() {
    final String host = SystemUtil.getLocalhostIp();
    this.messageCountUtil.SubmitConsumer(new Runnable() {

      @Override
      public void run() {
        try {
          sendConsumerMsgToMonitor(host);
        } catch (Exception ex) {
          LOG.error("detectorUtil.SubmitConsumer thread throw exception: " + ex.getMessage(), ex);
        }
      }
    });
  }

  private void sendConsumerMsgToMonitor(final String host) {
    List<MessageLog> list = new ArrayList<MessageLog>();
    Iterator<Consumer> keys = consumerToConsumerWorker.keySet().iterator();
    while (keys.hasNext()) {
      Consumer consumer = keys.next();
      MessageLog messageLog = new MessageLog();
      messageLog.setConsumerName(consumer.getConsumerId());
      messageLog.setTopicName(consumer.getDestination().getName());
      messageLog.setTime(System.currentTimeMillis());
      messageLog.setBroker(host);
      if (!KuroroUtil.isBlankString(consumer.getZone())) {
        messageLog.setZoneName(consumer.getZone());
      }
      List<OutMessagePair> outList = new ArrayList<OutMessagePair>();
      Map<String, AtomicLong> dequeueCount = consumerToConsumerWorker.get(consumer)
          .getConsumerIPdequeueCount();
      Map<String, AtomicLong> dequeueSize = consumerToConsumerWorker.get(consumer)
          .getConsumerIPdequeueSize();
      Iterator<String> subKeys = dequeueCount.keySet().iterator();
      while (subKeys.hasNext()) {
        String subKey = subKeys.next();
        Long deCount = dequeueCount.get(subKey).getAndSet(0L);
        if (deCount > 0) {
          //计算消息大小
          Long size = 0L;
          if (null != dequeueSize.get(subKey)) {
            size = dequeueSize.get(subKey).getAndSet(0L);
          }

          OutMessagePair outPair = new OutMessagePair();
          outPair.setConsumerIP(subKey);
          outPair.setCount(deCount);
          outPair.setSize(size);
          outList.add(outPair);
        }
      }
      if (!outList.isEmpty()) {
        messageLog.setOut(outList);
        list.add(messageLog);
      }
    }
    analystService.sendCounts(list);
  }

  private void updateMaxAckedMessageId(ConsumerWorker worker, Consumer consumer) {
    if (worker.getConsumerType() == ConsumerType.CLIENT_ACKNOWLEDGE) {
      Long lastSavedAckedMsgId = consumerToMaxSaveAckedMessageId.get(consumer);
      lastSavedAckedMsgId = lastSavedAckedMsgId == null ? 0 : lastSavedAckedMsgId;
      Long currentMaxAckedMsgId = worker.getMaxAckedMessageId();
      if (currentMaxAckedMsgId > 0 && currentMaxAckedMsgId > lastSavedAckedMsgId) {
        consumerToMaxSaveAckedMessageId.put(consumer, currentMaxAckedMsgId);
      }
    }
  }

  private void startMaxAckedMessageIdUpdaterThread() {
    maxAckedMessageIdUpdaterThread = threadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        while (!closed) {
          for (Map.Entry<Consumer, ConsumerWorker> entry : consumerToConsumerWorker.entrySet()) {
            ConsumerWorker worker = entry.getValue();
            Consumer consumer = entry.getKey();
            updateMaxAckedMessageId(worker, consumer);
          }
          try {
            Thread.sleep(consumerConfig.getMaxAckedMessageIdUpdateInterval());
          } catch (InterruptedException e) {
            break;
          }
        }
        LOG.info("MaxAckedMessageIdUpdaterThread closed");
      }
    }, "maxAckedMessageIdUpdaterThread-");
    maxAckedMessageIdUpdaterThread.start();
  }

  private void startIdleWorkerCheckerThread() {
    idleWorkerManagerCheckerThread = threadFactory.newThread(new Runnable() {

      @Override
      public void run() {
        while (!closed) {
          for (Map.Entry<Consumer, ConsumerWorker> entry : consumerToConsumerWorker.entrySet()) {
            try {
              shutdownWorker(entry.getValue(), entry.getKey());

            } catch (Throwable t) {
              LOG.error("Close idle consumer worker failed", t);
              t.printStackTrace();
            }
          }
          try {
            Thread.sleep(consumerConfig.getCheckConnectedChannelInterval());
          } catch (InterruptedException e) {
            break;
          }
        }
        LOG.info("idle ConsumerWorker checker thread closed");
      }
    }, "idleConsumerWorkerChecker-");
    idleWorkerManagerCheckerThread.start();
  }

  public void shutdownWorker(ConsumerWorker worker, Consumer consumer) {
    if (worker.allChannelDisconnected()) {
      updateMaxAckedMessageId(worker, consumer);
      workerDone(consumer);
      worker.closeMessageFetcherThread();
      worker.closeAckExecutor();
      worker.close();
      LOG.info("ConsumerWorker for " + consumer + " has no connected channel, close it");
    }
  }

  public void workerDone(Consumer consumer) {
    consumerToMaxSaveAckedMessageId.remove(consumer);
    consumerToConsumerWorker.remove(consumer);
  }

  public AckDAO getAckDAO() {
    return ackDAO;
  }

  public void setAckDAO(AckDAO ackDAO) {
    this.ackDAO = ProxyUtil.createMongoDaoProxyWithRetryMechanism(ackDAO,
        consumerConfig.getRetryIntervalWhenMongoException());
  }

  public MessageDAO getMessageDAO() {
    return messageDAO;
  }

  public void setMessageDAO(MessageDAO messageDAO) {
    this.messageDAO = ProxyUtil.createMongoDaoProxyWithRetryMechanism(messageDAO,
        consumerConfig.getRetryIntervalWhenMongoException());
  }

  public BlockingCenter getBlockingCenter() {
    return blockingCenter;
  }

  public void setBlockingCenter(BlockingCenter blockingCenter) {
    this.blockingCenter = blockingCenter;
  }

  public Map<Consumer, ConsumerWorker> getConsumerToConsumerWorker() {
    return consumerToConsumerWorker;
  }

  public Config getConsumerConfig() {
    return consumerConfig;
  }

  public KuroroThreadFactory getThreadFactory() {
    return threadFactory;
  }

  public JmxSpringUtil getJmxSpringUtil() {
    return jmxSpringUtil;
  }

  public void setJmxSpringUtil(JmxSpringUtil jmxSpringUtil) {
    this.jmxSpringUtil = jmxSpringUtil;
  }

  public MessageCountUtil getMessageCountUtil() {
    return messageCountUtil;
  }

  public void setMessageCountUtil(MessageCountUtil messageCountUtil) {
    this.messageCountUtil = messageCountUtil;
  }

  public AnalystService getAnalystService() {
    return analystService;
  }

  public void setAnalystService(AnalystService analystService) {
    this.analystService = analystService;
  }

}
