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

import com.epocharch.kuroro.common.inner.exceptions.NetException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderClient {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderClient.class);
  private final Long DEFAULT_TIMEOUT = 10000L;
  private ClientBootstrap bootstrap;
  private Channel channel;
  private String host;
  private int port = 20000;
  private String address;
  private int connectTimeout = 3000;
  private String topicName;
  private volatile boolean active = true;
  private volatile boolean activeSetable = false;
  private LeaderClientManager clientManager;
  private Map<Long, CallbackFuture> requestMap = new ConcurrentHashMap<Long, CallbackFuture>();
  //Compensation for asynchronous timeout messageId
  private ConcurrentHashMap<Long, MessageIDPair> removeTimeOutKeyMap = new ConcurrentHashMap<Long, MessageIDPair>();

  public LeaderClient(String host, int port, LeaderClientManager clientManager, String topicName) {
    this.host = host;
    this.port = port;
    this.address = host + ":" + port;
    this.clientManager = clientManager;

    clientManager.getCycExecutorService().scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          long now = System.currentTimeMillis();
          if (requestMap != null && requestMap.size() > 0) {
            for (Long key : requestMap.keySet()) {
              CallbackFuture requestData = requestMap.get(key);
              if (requestData != null) {
                // 超时移除
                if (requestData.getCreatedMillisTime() + DEFAULT_TIMEOUT < now) {
                  requestMap.remove(key);
                  LOG.warn("remove timeout key:" + key + " topicName:>>> " + requestData.getClient()
                      .getTopicName());
                }
              }
            }
          }
        } catch (Throwable t) {
          LOG.error(t.getMessage(), t);
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
    this.topicName = topicName;
    LOG.info("LeaderClient-Topic-" + topicName + "--->" + host + ":" + port);

  }

  public String getTopicName() {
    return this.topicName;
  }

  public synchronized void connect() throws NetException {
    if (channel != null && channel.isConnected()) {
      return;
    }
    if (bootstrap == null) {
      this.bootstrap = new ClientBootstrap(clientManager.getNioClientSocketChannelFactory());
      this.bootstrap.setPipelineFactory(
          new LeaderClientPipeLineFactory(this, this.clientManager.getClientResponseThreadPool()));
      bootstrap.setOption("tcpNoDelay", true);
    }

    try {
      ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
      // 等待连接创建成功
      if (future.awaitUninterruptibly(this.connectTimeout, TimeUnit.MILLISECONDS)) {
        if (future.isSuccess()) {
          LOG.info("Client is conneted to " + this.host + ":" + this.port);
        } else {
          LOG.warn("Client is not conneted to " + this.host + ":" + this.port);
        }
      }
      this.channel = future.getChannel();

      LOG.info("Channel is " + this.channel.isConnected());
    } catch (Exception e) {
      LOG.error("Connect error", e);
    }
  }

  public MessageIDPair sendTopicConsumer(WrapTopicConsumerIndex topicConsumerIndex) {
    try {
      CallbackFuture callback = new CallbackFuture();
      callback.setCreatedMillisTime(System.currentTimeMillis());
      write(callback, topicConsumerIndex);
      return callback.get(DEFAULT_TIMEOUT);
    } catch (NetException e) {
      LOG.error(e.getMessage(), e);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
    return null;
  }

  public MessageIDPair sleepLeaderWork(String topicName) {
    try {
      CallbackFuture callback = new CallbackFuture();
      callback.setCreatedMillisTime(System.currentTimeMillis());
      WrapTopicConsumerIndex topicConsumer = new WrapTopicConsumerIndex();
      topicConsumer.setTopicName(topicName);
      topicConsumer.setCommand("stop");
      write(callback, topicConsumer);
      return callback.get(DEFAULT_TIMEOUT);
    } catch (NetException e) {
      LOG.error(e.getMessage(), e);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
    return null;
  }

  private synchronized void write(CallbackFuture callback,
      WrapTopicConsumerIndex topicConsumerIndex) {
    if (channel == null) {
      LOG.error("channel:" + null + " ^^^^^^^^^^^^^^");
    } else {
      LOG.debug("Write leader request from {} for topic:" + topicName, channel.getLocalAddress());
      WrapTopicConsumerIndex topicConsumer = topicConsumerIndex;
      topicConsumer.setSquence(LeaderClientManager.sequence.getAndIncrement());
      ChannelFuture future = channel.write(topicConsumer);
      future.addListener(new MsgWriteListener(topicConsumer));
      callback.setFuture(future);
      callback.setClient(this);
      requestMap.put(topicConsumer.getSquence(), callback);
    }

  }

  public boolean isConnected() {
    return channel != null && channel.isConnected();
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    if (this.activeSetable) {
      this.active = active;
    }
  }

  public boolean isActiveSetable() {
    return activeSetable;
  }

  public void setActiveSetable(boolean activeSetable) {
    this.activeSetable = activeSetable;
  }

  public boolean isWritable() {
    return this.channel.isWritable();
  }

  public String getHost() {
    return host;
  }

  public String getAddress() {
    return this.address;
  }

  public int getPort() {
    return this.port;
  }

  public void doMessageIDPair(MessageIDPair messageIDPair) {
    CallbackFuture callBack = requestMap.get(messageIDPair.getSequence());
    if (callBack != null) {
      callBack.setMessageIDPair(messageIDPair);
      callBack.run();
      this.requestMap.remove(messageIDPair.getSequence());
    } else if (callBack == null && messageIDPair != null) {
      if (messageIDPair.getMinMessageId() != null && messageIDPair.getMaxMessageId() != null) {
        removeTimeOutKeyMap.put(messageIDPair.getSequence(), messageIDPair);
        LOG.warn("put  messageIDPair to removeTimeOutKeyMap >>> " + messageIDPair);
      }
    }
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean equals(Object obj) {
    if (obj instanceof LeaderClient) {
      LeaderClient nc = (LeaderClient) obj;
      return this.address.equals(nc.getAddress());
    } else {
      return super.equals(obj);
    }
  }

  public int hashCode() {
    return address.hashCode();
  }

  public void close() {
    if (channel != null && channel.isConnected()) {
      channel.close();
    }
    LOG.info("Leader is closed");
  }

  public ConcurrentHashMap<Long, MessageIDPair> getRemoveTimeOutKeyMap() {
    return removeTimeOutKeyMap;
  }

  public class MsgWriteListener implements ChannelFutureListener {

    private WrapTopicConsumerIndex wrapRet;

    public MsgWriteListener(WrapTopicConsumerIndex msg) {
      this.wrapRet = msg;
    }

    @Override
    public void operationComplete(ChannelFuture channelfuture) throws Exception {
      if (channelfuture.isSuccess()) {
        return;
      }
      MessageIDPair pa = new MessageIDPair();
      pa.setSequence(wrapRet.getSquence());
      pa.setTopicConsumerIndex(wrapRet.getTopicConsumerIndex());
      pa.setMaxMessageId(null);
      pa.setMinMessageId(null);
      doMessageIDPair(pa);
    }
  }

}
