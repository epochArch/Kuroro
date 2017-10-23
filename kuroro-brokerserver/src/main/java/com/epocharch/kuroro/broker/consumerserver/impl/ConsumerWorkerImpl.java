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


import com.epocharch.kuroro.broker.Config;
import com.epocharch.kuroro.broker.consumerserver.ACKHandlerType;
import com.epocharch.kuroro.broker.consumerserver.ConsumerWorker;
import com.epocharch.kuroro.broker.consumerserver.block.BlockingCenter;
import com.epocharch.kuroro.broker.consumerserver.block.CloseBlockingQueue;
import com.epocharch.kuroro.broker.consumerserver.block.MessageBlockingQueue;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.inner.strategy.PullStrategy;
import com.epocharch.kuroro.common.inner.util.IPUtil;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.jmx.support.JmxSpringUtil;
import com.epocharch.kuroro.common.message.Message;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

public class ConsumerWorkerImpl implements ConsumerWorker {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerWorkerImpl.class);
  public AtomicLong channelPollTimes = new AtomicLong();
  public AtomicLong pollTimes = new AtomicLong();
  public AtomicLong sendTimes = new AtomicLong();
  public AtomicLong sentSize = new AtomicLong();
  String idcZkPath = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH) + Constants.ZONE_TOPIC;
  private Consumer consumer;
  private BlockingQueue<Channel> freeChannels = new LinkedBlockingQueue<Channel>();
  private Map<Channel, String> connectedChannels = new ConcurrentHashMap<Channel, String>();
  private Thread messageFetcherThread;
  private CloseBlockingQueue<Message> messageQueue = null;
  private AckDAO ackDAO;
  private BlockingCenter blockingCenter;
  private KuroroThreadFactory threadFactory;
  private String consumerid;
  private Date createDate = new Date();
  private String topicName;
  private volatile boolean getMessageisAlive = true;
  private volatile boolean started = false;
  private ExecutorService ackExecutor;
  private PullStrategy pullStrategy;
  private Config consumerConfig;
  private Map<Channel, Map<WrappedMessage, Boolean>> waitAckMessage = new ConcurrentHashMap<Channel, Map<WrappedMessage, Boolean>>();
  private volatile long maxAckedMessageId = 0L;
  private MessageFilter messageFilter;
  private Map<String, AtomicLong> consumerIPdequeueCount = new ConcurrentHashMap<String, AtomicLong>();
  private Map<String, AtomicLong> consumerIPdequeueSize = new ConcurrentHashMap<String, AtomicLong>();
  private Queue<WrappedMessage> cachedMessages = new ConcurrentLinkedQueue<WrappedMessage>();
  private JmxSpringUtil jmxSpringUtil;
  private boolean needCompensation = false;
  private volatile boolean sendFlag = true;
  private volatile IZkDataListener listener;
  private MessageFilter consumeLocalZoneFilter;
  private String zone;
  private String idc;
  private String poolId;
  private volatile ConsumerOffset offset;

  public ConsumerWorkerImpl(Consumer consumer, ConsumerWorkerManager consumerWorkManager,
      MessageFilter messageFilter,
      MessageFilter consumeLocalZoneFilter, ConsumerOffset offset) {
    this.consumer = consumer;
    this.consumerConfig = consumerWorkManager.getConsumerConfig();
    this.ackDAO = consumerWorkManager.getAckDAO();
    this.blockingCenter = consumerWorkManager.getBlockingCenter();
    this.threadFactory = consumerWorkManager.getThreadFactory();
    this.jmxSpringUtil = consumerWorkManager.getJmxSpringUtil();
    this.messageFilter = messageFilter;
    this.topicName = consumer.getDestination().getName();
    this.consumerid = consumer.getConsumerId();
    this.consumeLocalZoneFilter = consumeLocalZoneFilter;
    this.zone = consumer.getZone();
    this.idc = consumer.getIdc();
    this.poolId = consumer.getPoolId();
    pullStrategy = new DefaultPullStrategy(consumerConfig.getPullFailDelayBase(),
        consumerConfig.getPullFailDelayUpperBound());
    ackExecutor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS,
        new LinkedBlockingQueue<Runnable>(),
        new KuroroThreadFactory("kuroro-ack-"));
    this.offset = offset;

    String mBeanName = (consumeLocalZoneFilter == null ?
        ("Consumer-" + topicName + '-' + consumerid) :
        ("Consumer-" + topicName + "-" + consumerid + "-" + zone));
    jmxSpringUtil.registerMBean(mBeanName, new MonitorBean(this));
    String idcTopicPath = idcZkPath + Constants.SEPARATOR + topicName;
    ZkClient idcZkClient = KuroroZkUtil.initIDCZk();
    try {
      TopicConfigDataMeta meta = null;
      if (idcZkClient.exists(idcTopicPath)) {
        meta = idcZkClient.readData(idcTopicPath, true);
      }

      if (meta != null && meta.isCompensate() != null) {
        // 主题配置了补偿且使用auto确认
        needCompensation = meta.isCompensate()
            && this.consumer.getConsumerType() == ConsumerType.CLIENT_ACKNOWLEDGE;
      }
      if (meta != null && isContainsConsumerid(meta, consumerid)) {
        sendFlag = false;
      }
      // watch compensation flag
      listener = new IZkDataListener() {
        @Override
        public void handleDataChange(String s, Object o) throws Exception {
          TopicConfigDataMeta meta = (TopicConfigDataMeta) o;
          if (meta != null && meta.isCompensate() != null) {
            // 主题配置了补偿且使用auto确认
            needCompensation = meta.isCompensate()
                && ConsumerWorkerImpl.this.consumer.getConsumerType()
                == ConsumerType.CLIENT_ACKNOWLEDGE;
            if (ConsumerWorkerImpl.this.messageQueue != null) {
              ConsumerWorkerImpl.this.messageQueue.setCompensation(needCompensation);
            }
          }
          if (meta != null && isContainsConsumerid(meta, consumerid)) {
            sendFlag = false;
          } else if (meta != null && !isContainsConsumerid(meta, consumerid)) {
            sendFlag = true;
          }
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {
        }
      };
      idcZkClient.subscribeDataChanges(idcTopicPath, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    startMessageFetcherThread();
    LOG.info("Consumer created {}", this);
  }

  public Map<Channel, String> getConnectedChannels() {
    return connectedChannels;
  }

  public void startMessageFetcherThread() {
    this.messageFetcherThread = threadFactory.newThread(new Runnable() {

      @Override
      public void run() {
        while (getMessageisAlive) {
          try {
            sendMessageByPollFreeChannelQueue();
          } catch (Throwable t) {
            LOG.error("messageFetcherThread error for " + topicName + ":" + consumerid, t);
          }
        }
        LOG.info("Message fetcher thread closed");

      }
    }, consumer.toString() + "-messagefetch-");
    this.messageFetcherThread.start();

  }

  private void updateWaitAckMessages(Channel channel, Long ackedMsgId) {
    if (ConsumerType.CLIENT_ACKNOWLEDGE.equals(consumer.getConsumerType())) {
      Map<WrappedMessage, Boolean> messages = waitAckMessage.get(channel);
      if (messages != null) {
        KuroroMessage kuroroMessage = new KuroroMessage();
        kuroroMessage.setMessageId(ackedMsgId);
        WrappedMessage wrappedMsg = new WrappedMessage(consumer.getDestination(), kuroroMessage);
        messages.remove(wrappedMsg);
      }
    }
  }

  private void updateMaxMessageId(Long ackedMsgId, Channel channel, ACKHandlerType type) {
    if (ackedMsgId != null && ConsumerType.CLIENT_ACKNOWLEDGE.equals(consumer.getConsumerType())) {
      LOG.info(
          "Receive ACK(" + topicName + "," + consumerid + "," + ackedMsgId + "," + type + ") from "
              + connectedChannels
              .get(channel));
      maxAckedMessageId = Math.max(maxAckedMessageId, ackedMsgId);
    }
  }

  @Override
  public void handleAck(final Channel channel, final Long ackedMsgId, final ACKHandlerType type) {
    ackExecutor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          updateWaitAckMessages(channel, ackedMsgId);
          updateMaxMessageId(ackedMsgId, channel, type);
          if (ACKHandlerType.CLOSE_CHANNEL.equals(type)) {
            LOG.info("Receive ack(type=" + type + ") from " + channel.getRemoteAddress());
            channel.close();
            return;
          } else if (ACKHandlerType.SEND_MESSAGE.equals(type) || ACKHandlerType.RESEND
              .equals(type)) {
            freeChannels.add(channel);
          }
          if (ConsumerType.CLIENT_ACKNOWLEDGE.equals(consumer.getConsumerType())
              && type != ACKHandlerType.RESEND) {
            String ip = connectedChannels.get(channel);
            if (consumeLocalZoneFilter != null) {
              ackDAO.add(topicName, consumerid, ackedMsgId, ip == null ? "nil" : ip, 0, idc, zone,
                  poolId);
            } else {
              ackDAO.add(topicName, consumerid, ackedMsgId, ip == null ? "nil" : ip, 0, idc, null,
                  poolId);//tricky nil
            }
          }
        } catch (Exception e) {
          LOG.error("HandleAck error!", e);
        }
      }
    });
  }

  @Override
  public synchronized void handleChannelDisconnect(Channel channel) {
    connectedChannels.remove(channel);
    if (allChannelDisconnected()) {

    }
    if (ConsumerType.CLIENT_ACKNOWLEDGE.equals(consumer.getConsumerType())) {
      Map<WrappedMessage, Boolean> msgMap = waitAckMessage.get(channel);
      if (msgMap != null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // netty自身的线程，不会有谁Interrupt
        }
        for (Map.Entry<WrappedMessage, Boolean> entry : msgMap.entrySet()) {
          cachedMessages.add(entry.getKey());
        }
        waitAckMessage.remove(channel);
      }
    }
  }

  private void sendMessageByPollFreeChannelQueue() {
    try {
      while (getMessageisAlive) {

        if (sendFlag) {
          Channel channel = freeChannels.take();
          channelPollTimes.incrementAndGet();
          // 如果未连接，则不做处理
          if (channel.isConnected()) {
            //创建消息缓冲QUEUE
            if (messageQueue == null) {
              try {
                messageQueue = blockingCenter
                    .createMessageQueue(topicName, consumerid, // messageIdOfTailMessage,
                        messageFilter, needCompensation, consumeLocalZoneFilter, zone, offset);
              } catch (RuntimeException e) {
                LOG.error("Error create message queue from BlockCenter!", e);
                freeChannels.add(channel);
                Thread.sleep(consumerConfig.getRetryIntervalWhenMongoException());
                continue;
              }
            }
            if (cachedMessages.isEmpty()) {
              putMsgToCachedMsgFromMsgQueue();
            }
            if (!cachedMessages.isEmpty()) {
              sendMsgFromCachedMessages(channel);
            }
          }
        } else {
          Thread.sleep(30000L);  //暂停状态，每隔30秒检查一次sendFlag消费暂停开关
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Message Fetcher Thread interrupted", e);
    }
  }

  private void sendMsgFromCachedMessages(Channel channel) throws InterruptedException {
    sendTimes.incrementAndGet();
    final WrappedMessage message = cachedMessages.poll();
    if (offset != null) {
      message.setRestOffset(offset.getRestOffset());
    }
    Long messageId = message.getContent().getMessageId();
    // 添加监控eventID
    String eventId = null;
    message.setMonitorEventID(eventId);
    try {
      channel.write(message).addListener(new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            cachedMessages.add(message);
          } else {
            sentSize.incrementAndGet();
          }
        }
      });

      String consumerIP = channel.getRemoteAddress().toString().substring(1).split(":")[0];
      // 消费数量
      if (consumerIPdequeueCount.containsKey(consumerIP)) {
        consumerIPdequeueCount.get(consumerIP).incrementAndGet();
      } else {
        consumerIPdequeueCount.put(consumerIP, new AtomicLong(1L));
      }
      // 消费内容大小
      if (consumerIPdequeueSize.containsKey(consumerIP)) {
        consumerIPdequeueSize.get(consumerIP).addAndGet(message.getContent().getContent().length());
      } else {
        consumerIPdequeueSize
            .put(consumerIP, new AtomicLong(message.getContent().getContent().length()));
      }
      // 如果是AT_MOST模式，收到ACK之前更新messageId的类型
      if (ConsumerType.AUTO_ACKNOWLEDGE.equals(consumer.getConsumerType())) {
        String ip = connectedChannels.get(channel);
        ackDAO.add(topicName, consumerid, messageId, ip == null ? "nil" : ip, 0, idc, null,
            poolId);//tricky nil

      }
      // 如果是AT_LEAST模式，发送完后，在server端记录已发送但未收到ACK的消息记录
      if (ConsumerType.CLIENT_ACKNOWLEDGE.equals(consumer.getConsumerType())) {
        Map<WrappedMessage, Boolean> messageMap = waitAckMessage.get(channel);
        if (channel.isConnected()) {
          if (messageMap == null) {
            messageMap = new ConcurrentHashMap<WrappedMessage, Boolean>();
            waitAckMessage.put(channel, messageMap);
          }
          messageMap.put(message, Boolean.TRUE);
        } else {
          LOG.warn("Cache a message " + message.getContent().getMessageId() + " of {},{}",
              topicName, consumerid);
          cachedMessages.add(message);
        }
      }

    } catch (Throwable e) {
      LOG.error(consumer.toString() + ":channel write error and cache it.", e);
      cachedMessages.add(message);
    }
  }

  private void putMsgToCachedMsgFromMsgQueue() throws InterruptedException {
    KuroroMessage message = null;
    if (getMessageisAlive) {
      while (getMessageisAlive) {
        message = (KuroroMessage) messageQueue
            .poll(pullStrategy.fail(false), TimeUnit.MILLISECONDS);
        if (message != null) {
          pullStrategy.succeess();
          break;
        }
      }
      pollTimes.incrementAndGet();
      if (message != null) {
        cachedMessages.add(new WrappedMessage(consumer.getDestination(), message));
      }
    }
  }

  private boolean isContainsConsumerid(TopicConfigDataMeta meta, String id) {
    if (meta.getSuspendConsumerIdList() != null && meta.getSuspendConsumerIdList().contains(id)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void closeMessageFetcherThread() {
    getMessageisAlive = false;
    if (this.messageFetcherThread != null && this.messageFetcherThread.isAlive()
        && !this.messageFetcherThread.isInterrupted()) {
      this.messageFetcherThread.interrupt();
    }
  }

  @Override
  public void closeAckExecutor() {
    ackExecutor.shutdownNow();
  }

  @Override
  public void close() {
    closeMessageFetcherThread();
    try {
      if (messageQueue != null) {
        messageQueue.close();
      }
      String idcTopicPath = idcZkPath + Constants.SEPARATOR + topicName;
      ZkClient idcZkClient = KuroroZkUtil.initIDCZk();
      if (idcZkClient.exists(idcTopicPath)) {
        idcZkClient.unsubscribeDataChanges(idcTopicPath, listener);
      }
    } catch (Exception e) {
      LOG.warn("meesageQueue of consumerWorker is closeing happen exception! topic:" + topicName
          + ", consumerId:" + consumer, e);
    }
  }

  @Override
  public boolean allChannelDisconnected() {
    return started && connectedChannels.isEmpty();
  }

  @Override
  public long getMaxAckedMessageId() {
    return maxAckedMessageId;
  }

  @Override
  public ConsumerType getConsumerType() {
    return consumer.getConsumerType();
  }

  public Map<Channel, Map<WrappedMessage, Boolean>> getWaitAckMessage() {
    return waitAckMessage;
  }

  public Queue<WrappedMessage> getCachedMessages() {
    return cachedMessages;
  }

  @Override
  public void handleSpreed(final Channel channel, final int clientThreadCount) {
    ackExecutor.execute(new Runnable() {
      @Override
      public void run() {
        connectedChannels.put(channel, IPUtil.getIpFromChannel(channel));
        started = true;
        for (int i = 0; i < clientThreadCount; i++) {
          freeChannels.add(channel);
        }
        List<Channel> list = new ArrayList<Channel>();
        freeChannels.drainTo(list);
        Collections.shuffle(list);
        freeChannels.addAll(list);
      }
    });
  }

  @Override
  public String toString() {
    return "ConsumerWorkerImpl[" + "consumer=" + consumer + ", consumerid='" + consumerid + '\''
        + ", topicName='" + topicName + '\''
        + ", getMessageisAlive=" + getMessageisAlive + ", started=" + started + ",sendFlag="
        + sendFlag + ", needCompensation="
        + needCompensation + ",consumeLocalZoneFilter=" + consumeLocalZoneFilter
        + ", consumerOffset=" + offset + ']';
  }

  public Map<String, AtomicLong> getConsumerIPdequeueCount() {
    return consumerIPdequeueCount;
  }

  public Map<String, AtomicLong> getConsumerIPdequeueSize() {
    return consumerIPdequeueSize;
  }

  @Override
  public ConsumerOffset getOffset() {
    return offset;
  }

  @ManagedResource(description = "consumer info in running")
  public static class MonitorBean {

    private final WeakReference<ConsumerWorkerImpl> consumerWorkerImpl;

    private MonitorBean(ConsumerWorkerImpl workImpl) {
      this.consumerWorkerImpl = new WeakReference<ConsumerWorkerImpl>(workImpl);
    }

    @ManagedAttribute
    public String getConnectedChannels() {
      if (consumerWorkerImpl.get() != null) {
        StringBuilder sb = new StringBuilder();
        if (consumerWorkerImpl.get().connectedChannels != null) {
          for (Channel channel : consumerWorkerImpl.get().connectedChannels.keySet()) {
            sb.append(channel.getRemoteAddress()).append("(isConnected:")
                .append(channel.isConnected()).append(')');
          }
        }
        return sb.toString();
      }
      return null;
    }

    @ManagedAttribute
    public String getFreeChannels() {
      if (consumerWorkerImpl.get() != null) {
        StringBuilder sb = new StringBuilder();
        if (consumerWorkerImpl.get().freeChannels != null) {
          for (Channel channel : consumerWorkerImpl.get().freeChannels) {
            sb.append(channel.getRemoteAddress()).append("(isConnected:")
                .append(channel.isConnected()).append(')');
          }
        }
        return sb.toString();
      }
      return null;
    }

    @ManagedAttribute
    public Long getFetchTimes() {
      if (consumerWorkerImpl.get() != null && consumerWorkerImpl.get().messageQueue != null) {
        return ((MessageBlockingQueue) consumerWorkerImpl.get().messageQueue).fetchTimes.get();
      }
      return null;
    }

    @ManagedAttribute
    public Long getFetchSize() {
      if (consumerWorkerImpl.get() != null && consumerWorkerImpl.get().messageQueue != null) {
        return ((MessageBlockingQueue) consumerWorkerImpl.get().messageQueue).fetchSize.get();
      }
      return null;
    }

    @ManagedAttribute
    public Long getChannelPollTimes() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().channelPollTimes.get();
      }
      return null;
    }

    @ManagedAttribute
    public Long getPollTimes() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().pollTimes.get();
      }
      return null;
    }

    @ManagedAttribute
    public Long getSendTimes() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().sendTimes.get();
      }
      return null;
    }

    @ManagedAttribute
    public Long getSentSize() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().sentSize.get();
      }
      return null;
    }

    @ManagedAttribute
    public String getConsumerInfo() {
      if (consumerWorkerImpl.get() != null) {
        return "ConsumerId=" + consumerWorkerImpl.get().consumer.getConsumerId() + ",ConsumerType="
            + consumerWorkerImpl
            .get().consumer.getConsumerType();
      }
      return null;

    }

    @ManagedAttribute
    public String getConsumerId() {
      if (consumerWorkerImpl.get() != null) {
        MessageFilter consumerLocalFilter = consumerWorkerImpl.get().consumeLocalZoneFilter;
        if (consumerLocalFilter != null) {
          String zone = consumerWorkerImpl.get().zone;
          if (!KuroroUtil.isBlankString(zone)) {
            return consumerWorkerImpl.get().consumer.getConsumerId() + "#" + zone;
          }
        }
        return consumerWorkerImpl.get().consumer.getConsumerId();
      }
      return null;
    }

    @ManagedAttribute
    public String getTopicName() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().topicName;
      }
      return null;
    }

    @ManagedAttribute
    public String getCachedMessages() {
      if (consumerWorkerImpl.get() != null) {
        if (consumerWorkerImpl.get().cachedMessages != null) {
          return consumerWorkerImpl.get().cachedMessages.toString();
        }
      }
      return null;
    }

    @ManagedAttribute
    public Boolean getNeedCompensation() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().needCompensation;
      }
      return null;
    }

    @ManagedAttribute
    public String getWaitAckMessages() {
      if (consumerWorkerImpl.get() != null) {
        StringBuilder sb = new StringBuilder();
        if (consumerWorkerImpl.get().waitAckMessage != null) {
          for (Entry<Channel, Map<WrappedMessage, Boolean>> waitAckMessage : consumerWorkerImpl
              .get().waitAckMessage.entrySet()) {
            if (waitAckMessage.getValue().size() != 0) {
              sb.append(waitAckMessage.getKey().getRemoteAddress())
                  .append(waitAckMessage.getValue().toString());
            }
          }
        }
        return sb.toString();
      }
      return null;
    }

    @ManagedAttribute
    public String getCreateDate() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().createDate.toString();
      }
      return null;
    }

    @ManagedAttribute
    public Boolean getIsMessageisAlive() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().getMessageisAlive;
      }
      return null;
    }

    @ManagedAttribute
    public Boolean getIsStarted() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().started;
      }
      return null;
    }

    @ManagedAttribute
    public String getMaxAckedMessageId() {
      if (consumerWorkerImpl.get() != null) {
        return Long.toString(consumerWorkerImpl.get().maxAckedMessageId);
      }
      return null;

    }

    @ManagedAttribute
    public String getMessageFilter() {
      if (consumerWorkerImpl.get() != null) {
        if (consumerWorkerImpl.get().messageFilter != null) {
          return consumerWorkerImpl.get().messageFilter.toString();
        }
      }
      return null;
    }

    @ManagedAttribute
    public Boolean getIsSendFlag() {
      if (consumerWorkerImpl.get() != null) {
        return consumerWorkerImpl.get().sendFlag;
      }
      return null;
    }

    @ManagedAttribute
    public String getConsumeLocalZoneFilter() {
      if (consumerWorkerImpl.get() != null) {
        if (consumerWorkerImpl.get().consumeLocalZoneFilter != null) {
          return consumerWorkerImpl.get().consumeLocalZoneFilter.toString();
        }
      }
      return null;
    }

    @ManagedAttribute
    public String getOffset() {
      if (consumerWorkerImpl.get() != null) {
        if (consumerWorkerImpl.get().offset != null) {
          return consumerWorkerImpl.get().offset.toString();
        }
      }
      return null;
    }
  }
}
