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

import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.jmx.support.JmxSpringUtil;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

public class LeaderWorkerManager {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderWorkerManager.class);
  private ConcurrentHashMap<WrapTopicConsumerIndex, LeaderWorker> topicConsumerIndexToLeaderWorker = new ConcurrentHashMap<WrapTopicConsumerIndex, LeaderWorker>();
  private MessageDAO messageDAO;
  private JmxSpringUtil jmxSpringUtil;
  private AckDAO ackDAO;
  private List<String> topics;
  private Compensator compensator;
  private Thread idleLeaderWorkerManagerCheckerThread;
  private KuroroThreadFactory threadFactory = new KuroroThreadFactory();
  private volatile boolean closed = false;

  public void init() throws Exception {
    startIdleLeaderWorkerCheckerThread();
    compensator.start();
  }

  public LeaderWorker findOrCreateLeaderWork(WrapTopicConsumerIndex topicConsumer,
      boolean isCompensate) {
    LeaderWorker worker = topicConsumerIndexToLeaderWorker.get(topicConsumer);
    if (worker == null) {
      synchronized (topicConsumerIndexToLeaderWorker) {
        if ((worker = topicConsumerIndexToLeaderWorker.get(topicConsumer)) == null) {
          worker = new LeaderWorkerImpl(topicConsumer, this);
          worker.setRequestLeaderTime(System.currentTimeMillis());
          topicConsumerIndexToLeaderWorker.put(topicConsumer, worker);
          if (isCompensate) {
            worker.startCompensation();
          }
        }
      }
    } else {
      if (worker.isClosed()) {
        synchronized (topicConsumerIndexToLeaderWorker) {
          topicConsumerIndexToLeaderWorker.remove(topicConsumer);
        }
        return null;
      } else {
        String topic = topicConsumer.getTopicName();
        String consumer = topicConsumer.getConsumerId();
        String zone = topicConsumer.getZone();
        topicConsumerIndexToLeaderWorker.get(topicConsumer)
            .setRequestLeaderTime(System.currentTimeMillis());

        if (!isCompensate && compensator.isCompensating(topic, consumer, zone)) {
          LOG.info("Stop compensation after leader found[topic={},consumer={},zone= " + zone + "]",
              topic, consumer);
          compensator.stopCompensation(topic, consumer, zone);
        }
        if (isCompensate && !compensator.isCompensating(topic, consumer, zone)) {
          LOG.info("Start compensation after leader found[topic={},consumer={},zone= " + zone + "]",
              topic, consumer);
          worker.startCompensation();
        }
      }
    }

    return worker;
  }

  private void startIdleLeaderWorkerCheckerThread() {
    idleLeaderWorkerManagerCheckerThread = threadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        while (!closed) {
          for (Map.Entry<WrapTopicConsumerIndex, LeaderWorker> entry : topicConsumerIndexToLeaderWorker
              .entrySet()) {
            try {
              shutdownLeaderWorker(entry.getKey(), entry.getValue());

            } catch (Exception t) {
              LOG.error("Close idle leader worker failed", t);
              t.printStackTrace();
            }
          }
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            LOG.error("idle leader worker sleep thread interruptexception ", e);
            break;
          }
        }
        LOG.info("idle ConsumerWorker checker thread closed");
      }
    }, "idleLeaderWorkerChecker-");
    idleLeaderWorkerManagerCheckerThread.start();
  }

  public void shutdownLeaderWorker(WrapTopicConsumerIndex topicConsumerIndex, LeaderWorker worker) {
    if (worker != null) {
      Long currentTime = System.currentTimeMillis();
      Long updateTime = worker.getRequestLeaderTime();
      if (currentTime == null || updateTime == null) {
        return;
      } else if (((currentTime - updateTime) / (60 * 1000)) >= 10) {
        worker.close();
        synchronized (topicConsumerIndexToLeaderWorker) {
          topicConsumerIndexToLeaderWorker.remove(topicConsumerIndex);
          LOG.warn("====close idle leaderworker =====" + topicConsumerIndex);
        }
      }
    }
  }

  public void closeLeaderWork(WrapTopicConsumerIndex topicConsumerIndex) {
    LeaderWorker worker = topicConsumerIndexToLeaderWorker.get(topicConsumerIndex);
    if (worker != null) {
      worker.close();
      synchronized (topicConsumerIndexToLeaderWorker) {
        topicConsumerIndexToLeaderWorker.remove(topicConsumerIndex);
      }
    }
  }

  public void closeLeaderWorkByTopic(String topicName) {
    synchronized (topicConsumerIndexToLeaderWorker) {
      Iterator<WrapTopicConsumerIndex> keys = topicConsumerIndexToLeaderWorker.keySet().iterator();
      while (keys.hasNext()) {
        WrapTopicConsumerIndex key = keys.next();
        if (key.getTopicName().equals(topicName)) {
          LeaderWorker worker = topicConsumerIndexToLeaderWorker.get(key);
          if (worker != null) {
            worker.close();
          }
          keys.remove();
        }
      }
    }
  }

  public void close() {
    for (Map.Entry<WrapTopicConsumerIndex, LeaderWorker> entry : topicConsumerIndexToLeaderWorker
        .entrySet()) {
      entry.getValue().close();
    }
    this.compensator.close();// 关闭补偿器
    LOG.info("close leader worker");
  }

  public ConcurrentHashMap<WrapTopicConsumerIndex, LeaderWorker> getTopicConsumerIndexToLeaderWorker() {
    return topicConsumerIndexToLeaderWorker;
  }

  public MessageDAO getMessageDAO() {
    return messageDAO;
  }

  public void setMessageDAO(MessageDAO messageDAO) {
    this.messageDAO = messageDAO;
  }


  /*public void setMessageDAO(MessageDAO messageDAO) {
    this.messageDAO = ProxyUtil.createMongoDaoProxyWithRetryMechanism(messageDAO, 500L);
  }*/

  public JmxSpringUtil getJmxSpringUtil() {
    return jmxSpringUtil;
  }

  public void setJmxSpringUtil(JmxSpringUtil jmxSpringUtil) {
    this.jmxSpringUtil = jmxSpringUtil;
    String mBeanName = "LeaderWorkerManager-Topic-Info";
    jmxSpringUtil.registerMBean(mBeanName, new MonitorBean(this));
  }

  public Compensator getCompensator() {
    return compensator;
  }

  public void setCompensator(Compensator compensator) {
    this.compensator = compensator;
  }

  public AckDAO getAckDAO() {
    return ackDAO;
  }

  public void setAckDAO(AckDAO ackDAO) {
    this.ackDAO = ackDAO;
  }

/*  public void setAckDAO(AckDAO ackDAO) {
    this.ackDAO = ackDAO;
    //this.ackDAO = ProxyUtil.createMongoDaoProxyWithRetryMechanism(ackDAO, 500L);
  }*/

  @ManagedResource(description = "leaderWork list info")
  public static class MonitorBean {

    private final WeakReference<LeaderWorkerManager> leaderWorkerManager;

    private MonitorBean(LeaderWorkerManager workerManager) {
      this.leaderWorkerManager = new WeakReference<LeaderWorkerManager>(workerManager);
    }

    @ManagedAttribute
    public String getLeaderWorkers() {
      if (this.leaderWorkerManager.get() != null) {
        StringBuilder builder = new StringBuilder();
        for (LeaderWorker worker : this.leaderWorkerManager.get()
            .getTopicConsumerIndexToLeaderWorker().values()) {
          builder.append(worker.toString());
        }
        return builder.toString();
      }
      return null;
    }

  }

}
