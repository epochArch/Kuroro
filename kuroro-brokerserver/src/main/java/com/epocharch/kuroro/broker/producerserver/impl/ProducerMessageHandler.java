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

package com.epocharch.kuroro.broker.producerserver.impl;

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.util.IPUtil;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.SHAUtil;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedProducerAck;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
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

public class ProducerMessageHandler extends SimpleChannelUpstreamHandler {

  public static final int OK = 200;
  public static final int EXECUTOR_SIZE_PER_MONGO = 50;
  public static final int INVALID_TOPIC_NAME = 201;
  public static final int SAVE_FAILED = 202;
  private static final Logger LOG = LoggerFactory.getLogger(ProducerMessageHandler.class);
  private static final Map<String, Integer> levelMap = new ConcurrentHashMap<String, Integer>();
  /**
   * Mongo地址->ExecutorService映射
   */
  private static final ConcurrentHashMap<String, ExecutorService> mongoExecutorMap = new ConcurrentHashMap<String, ExecutorService>();
  /**
   * Mongo地址->ExecutorService映射
   */
  private static final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<String, ExecutorService>();
  /**
   * 默认ExecutorService
   */
  private static final ExecutorService defaultExecutor = Executors
      .newFixedThreadPool(EXECUTOR_SIZE_PER_MONGO,
          new DefaultThreadFactory("kuroro-mongo-writer-default", true));
  private static ChannelGroup channelGroup = new DefaultChannelGroup();
  private final MessageDAO messageDAO;
  private List<String> topics = null;
  private String localIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);

  private String idcZkRoot = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);

  private String idcZkTopicPath = null;

  public ProducerMessageHandler(MessageDAO messageDAO) throws Exception {
    this.messageDAO = messageDAO;
    initMeta();
  }

  public static ChannelGroup getChannelGroup() {
    return channelGroup;
  }

  private void initMeta() throws Exception {
    LOG.info("Initializing meta...");
    ZkClient zk = KuroroZkUtil.initIDCZk();
    idcZkTopicPath = idcZkRoot + Constants.ZONE_TOPIC;
    if (zk.exists(idcZkTopicPath)) {
      topics = zk.getChildren(idcZkTopicPath);
      if (topics == null || topics.isEmpty()) {
        LOG.warn("can't find topic meta from zookeeper!");
      }
    } else {
      zk.createPersistent(idcZkTopicPath, true);
    }
    for (String topic : topics) {
      reloadMeta(topic);
      subscribeMeta(topic);
    }
    zk.subscribeChildChanges(idcZkTopicPath, new IZkChildListener() {
      @Override
      public void handleChildChange(String s, List<String> list) throws Exception {
        for (String t : topics) {
          if (!list.contains(t)) {
            LOG.info("Remove meta info of {}", t);
            executors.remove(t);
            levelMap.remove(t);
          }
        }
        for (String t : list) {
          if (!topics.contains(t)) {
            reloadMeta(t);
            subscribeMeta(t);
          }
        }
        topics = list;
      }
    });
  }

  private void reloadMeta(final String topic) throws Exception {
    LOG.info("Reloading meta of {}", topic);
    ZkClient zk = KuroroZkUtil.initIDCZk();
    final String path = idcZkTopicPath + Constants.SEPARATOR + topic;
    TopicConfigDataMeta meta = zk.readData(path);
    if (meta != null) {
      int level = meta.getLevel() == null ? 0 : meta.getLevel();
      String mongo = meta.getReplicationSetList().toString();
      if (mongo != null && !mongoExecutorMap.containsKey(mongo)) {
        mongoExecutorMap.put(mongo, Executors.newFixedThreadPool(EXECUTOR_SIZE_PER_MONGO,
            new DefaultThreadFactory("kuroro-mongo-writer-" + identifyMongo(mongo), true)));
      }
      executors.put(topic, mongoExecutorMap.get(mongo));
      levelMap.put(topic, level);
    }
  }

  private String identifyMongo(String addr) {
    if (addr == null) {
      return "default";
    }
    return StringUtils.mid(DigestUtils.md5Hex(addr), 7, 16);
  }

  private void subscribeMeta(final String topic) throws Exception {
    LOG.info("Subscribing meta of {}", topic);
    final String path = idcZkTopicPath + Constants.SEPARATOR + topic;
    KuroroZkUtil.initIDCZk().subscribeDataChanges(path, new IZkDataListener() {
      @Override
      public void handleDataChange(String s, Object o) throws Exception {
        LOG.info("Data change of {}", topic);
        reloadMeta(topic);
      }

      @Override
      public void handleDataDeleted(String s) throws Exception {
        LOG.info("Data deleted of {}", topic);
        levelMap.remove(topic);
        executors.remove(topic);
      }
    });
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) {

    if (e instanceof ChannelStateEvent) {
      LOG.info(e.toString());
    }
    try {
      super.handleUpstream(ctx, e);
    } catch (Exception e1) {
      LOG.warn("Handle Upstrem Exceptions.", e);
    }
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final WrappedMessage receivedData = (WrappedMessage) e.getMessage();
    final String topicName = receivedData.getDestination().getName();
    ExecutorService executorService = null;

    if (executors.containsKey(topicName)) {
      executorService = executors.get(topicName);
    } else {
      LOG.warn("Default executor selected of {}", topicName);
      executorService = defaultExecutor;
    }

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Wrap wrapRet = null;
        KuroroMessage kuroroMessage;
        final String sha1;
        String topicKey = null;
        switch (receivedData.getWrappedType()) {
          case OBJECT_MSG:
            kuroroMessage = receivedData.getContent();

            sha1 = SHAUtil.generateSHA(kuroroMessage.getContent());
            kuroroMessage.setSha1(sha1);

            try {
              // 检查写入级别
              int level = getWriteLevel(topicName);
              if (!KuroroUtil.isBlankString(kuroroMessage.getZone())) {
                topicKey = topicName + "#" + kuroroMessage.getZone();
              } else {
                topicKey = topicName;
              }
              // 注册监控(数量)
              if (ProducerServer.getTopicMapCount().containsKey(topicKey)) {
                ProducerServer.getTopicMapCount().get(topicKey).incrementAndGet();
              } else {
                ProducerServer.getTopicMapCount().put(topicKey, new AtomicLong(1));
              }
              // 注册监控(消息大小)
              if (ProducerServer.getTopicMapSize().containsKey(topicKey)) {
                ProducerServer.getTopicMapSize().get(topicKey)
                    .addAndGet(kuroroMessage.getContent().length());
              } else {
                ProducerServer.getTopicMapSize()
                    .put(topicKey, new AtomicLong(kuroroMessage.getContent().length()));
              }

              // 保存
              wrapRet = new WrappedProducerAck(sha1, OK);
              wrapRet.setSequence(receivedData.getSequence());

              String ip = IPUtil.getIpFromChannel(e.getChannel());
              if (ip != null) {
                kuroroMessage.setSourceIp(ip);
              }
              messageDAO.saveMessage(topicName, kuroroMessage, level);
            } catch (Exception t) {
              // 记录异常，返回失败ACK，reason是“Can not save message”
              t.printStackTrace();
              LOG.error("[Save message to DB failed.]===" + topicKey, t);
              wrapRet = new WrappedProducerAck("Can not save message." + t.getMessage(),
                  SAVE_FAILED);
              wrapRet.setSequence(receivedData.getSequence());

            }
            break;
          case TRANSFER_MSG_BATCH://批量transfer
            List<KuroroMessage> kuroroMessages = receivedData.getBatchContent();
            int idx = -1;//标记写到第几个消息
            try {
              // 检查写入级别
              int level = getWriteLevel(topicName);

              int size = kuroroMessages.size();

              // 注册监控(消息大小)
              Long batchSize = 0L;
              for (int i = 0; i < size; i++) {
                batchSize += kuroroMessages.get(i).getContent().length();
                String zone = kuroroMessages.get(i).getZone();
                if (!KuroroUtil.isBlankString(zone)) {
                  topicKey = topicName + "#" + kuroroMessages.get(i).getZone();
                } else {
                  topicKey = topicName;
                }
                // 注册监控 (数量)
                if (ProducerServer.getTopicMapCount().containsKey(topicKey)) {
                  ProducerServer.getTopicMapCount().get(topicKey).incrementAndGet();
                } else {
                  ProducerServer.getTopicMapCount().put(topicKey, new AtomicLong(1));
                }
                if (ProducerServer.getTopicMapSize().containsKey(topicKey)) {
                  ProducerServer.getTopicMapSize().get(topicKey).addAndGet(batchSize);
                } else {
                  ProducerServer.getTopicMapSize().put(topicKey, new AtomicLong(batchSize));
                }
              }

              // 保存
              wrapRet = new WrappedProducerAck("OK", OK);//tricky:批量发暂时不返回校验码
              wrapRet.setSequence(receivedData.getSequence());
              for (KuroroMessage message : kuroroMessages) {
                messageDAO.transferMessage(topicName, message, level);
                idx++;
              }
            } catch (Exception t) {
              LOG.error("[Save message to DB failed.]===" + topicKey, t);
              wrapRet = new WrappedProducerAck(Integer.toString(idx), SAVE_FAILED);
              wrapRet.setSequence(receivedData.getSequence());

            }
            break;
          case TRANSFER_MSG:
            kuroroMessage = receivedData.getContent();
            if (!KuroroUtil.isBlankString(kuroroMessage.getZone())) {
              topicKey = topicName + "#" + kuroroMessage.getZone();
            } else {
              topicKey = topicName;
            }
            try {
              // 注册监控 (数量)
              if (ProducerServer.getTopicMapCount().containsKey(topicKey)) {
                ProducerServer.getTopicMapCount().get(topicKey).incrementAndGet();
              } else {
                ProducerServer.getTopicMapCount().put(topicKey, new AtomicLong(1));
              }
              // 注册监控(消息大小)
              if (ProducerServer.getTopicMapSize().containsKey(topicKey)) {
                ProducerServer.getTopicMapSize().get(topicKey)
                    .addAndGet(kuroroMessage.getContent().length());
              } else {
                ProducerServer.getTopicMapSize()
                    .put(topicKey, new AtomicLong(kuroroMessage.getContent().length()));
              }

              wrapRet = new WrappedProducerAck(kuroroMessage.getSha1(), OK);
              wrapRet.setSequence(receivedData.getSequence());
              messageDAO.transferMessage(topicName, kuroroMessage, 1);
            } catch (Exception t) {
              // 记录异常，返回失败ACK，reason是“Can not save message”
              LOG.error("[Save message to DB failed.]===" + topicKey, t);
              wrapRet = new WrappedProducerAck("Can not save message." + t.getMessage(),
                  SAVE_FAILED);
              wrapRet.setSequence(receivedData.getSequence());

            }
            break;
          default:
            LOG.warn("[Received unrecognized wrap.]" + receivedData);
            break;
        }
        if (receivedData.isACK()) {
          e.getChannel().write(wrapRet);
        }

      }
    });
  }

  private int getWriteLevel(final String topicName) throws Exception {
    int level = 0;
    if (levelMap.containsKey(topicName)) {
      level = levelMap.get(topicName);
    }
    return level;
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
