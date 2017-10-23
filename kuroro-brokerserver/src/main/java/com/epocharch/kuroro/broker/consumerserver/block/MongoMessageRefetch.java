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

import com.epocharch.kuroro.broker.leaderserver.LeaderClient;
import com.epocharch.kuroro.broker.leaderserver.LeaderClientManager;
import com.epocharch.kuroro.broker.leaderserver.MessageIDPair;
import com.epocharch.kuroro.broker.leaderserver.WrapTopicConsumerIndex;
import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.consumer.MessageFilter.FilterType;
import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoMessageRefetch implements MessageRefetch {

  private static final Logger LOG = LoggerFactory.getLogger(MongoMessageRefetch.class);
  private static final String DEFAULT_IP = "0.0.0.0";
  private final int fetchSize = Integer
      .parseInt(System.getProperty("global.block.queue.fetchsize", "200"));
  private final int RetryCount = 2;
  private MessageDAO messageDAO;
  private AckDAO ackDAO;
  private LeaderClientManager leaderClientManager;

  public void setLeaderClientManager(LeaderClientManager leaderClientManager) {
    this.leaderClientManager = leaderClientManager;
  }

  public void setMessageDAO(MessageDAO messageDAO) {
    this.messageDAO = messageDAO;
  }

  public void setAckDAO(AckDAO ackDAO) {
    this.ackDAO = ackDAO;
  }

  @Override
  public List refetchMessage(String topicName, String consumerId, MessageFilter messageFilter,
      boolean needCompensation,
      MessageFilter consumeLocalZoneFilter, String zone, ConsumerOffset offset) {
    List<KuroroMessage> messageIDList = null;
    int index = 0;
    WrapTopicConsumerIndex topicConsumerIndex = new WrapTopicConsumerIndex();
    topicConsumerIndex.setTopicName(topicName);
    topicConsumerIndex.setIndex(index);
    topicConsumerIndex.setConsumerId(consumerId);
    topicConsumerIndex
        .setCommand(needCompensation ? WrapTopicConsumerIndex.CMD_COMPENSATION : null);
    if (consumeLocalZoneFilter != null) {
      topicConsumerIndex.setZone(zone);
    }
    if (offset != null) {
      topicConsumerIndex.setOffset(offset);
    }
    try {
      LeaderClient client = leaderClientManager.getLeaderClient(topicConsumerIndex);
      MessageIDPair pair = null;
      Iterator<Entry<Long, MessageIDPair>> it = null;
      if (client != null) {
        if (client.getRemoveTimeOutKeyMap() != null && client.getRemoveTimeOutKeyMap().size() > 0) {
          it = client.getRemoveTimeOutKeyMap().entrySet().iterator();
          Map.Entry<Long, MessageIDPair> entry = it.next();
          pair = entry.getValue();
          LOG.warn(
              topicName + " get removeTimeOutMap pair topic#consumer >>> " + topicConsumerIndex
                  .getTopicConsumerIndex()
                  + " pair : " + pair + " >>> " + entry.getKey());
          it.remove();
          LOG.warn("remove this key topic#consumer: " + pair);
        } else {
          pair = client.sendTopicConsumer(topicConsumerIndex);
          if (offset != null) {
            offset.setRestOffset(pair.getRestOffset());
          }
        }
        if (pair != null) {
          if (!pair.getTopicConsumerIndex().equals(topicConsumerIndex.getTopicConsumerIndex())) {
            LOG.error(pair.getTopicConsumerIndex() + " is not match " + topicConsumerIndex
                .getTopicConsumerIndex());
            return null;
          }
          if (pair.getMaxMessageId() != null && pair.getMinMessageId() != null) {
            int retryCount = 0;
            boolean isCompensation = false;
            if (pair.getCommand() != null && pair.getCommand().equals(MessageIDPair.COMPENSATION)) {
              // 如果cmd为补偿
              isCompensation = true;
            }
            do {
              try {
                if (isCompensation) {
                  messageIDList = messageDAO
                      .getMessagesBetween(topicName, pair.getMinMessageId(), pair.getMaxMessageId(),
                          messageFilter,
                          index, consumeLocalZoneFilter);
                } else {
                  messageIDList = messageDAO
                      .getMessagesGreaterThan(topicName, pair.getMinMessageId(),
                          pair.getMaxMessageId(),
                          messageFilter, index, consumeLocalZoneFilter);
                }
                retryCount = RetryCount;
              } catch (Exception me) {
                retryCount++;
                if (retryCount == RetryCount) {
                  LOG.error(me.getMessage(), me.getCause());
                }
                client.getRemoveTimeOutKeyMap().put(pair.getSequence(), pair);
                LOG.info("get mongo messageIDList is error! put this pair " + pair
                    + "  into  RemoveTimeOutKeyMap! ");
              }
            } while (retryCount < RetryCount);

            if (messageIDList != null && messageIDList.size() <= fetchSize) {
              if (messageFilter != null && messageFilter.getType() == FilterType.InSet
                  && messageFilter.getParam() != null
                  && !messageFilter.getParam().isEmpty()) {
                if (needCompensation) {
                  // 为了不和补偿冲突，应记录不合条件的消息的ack
                  // 获取所有的ID
                  List<Long> ids = null;

                  if (isCompensation) {
                    ids = messageDAO.getMessageIdBetween(topicName, pair.getMinMessageId(),
                        pair.getMaxMessageId(), null
                        /** 取所有ID **/, index);
                  } else {
                    ids = messageDAO
                        .getMessagesIdGreaterThan(topicName, pair.getMinMessageId(),
                            pair.getMaxMessageId(), null,
                            index);
                  }
                  if (ids != null && !ids.isEmpty() && messageIDList != null) {
                    // 去掉需要的
                    for (KuroroMessage msg : messageIDList) {
                      Long id = msg.getMessageId();
                      ids.remove(id);
                    }
                    // 记录不需要的ID到ack表
                    for (Long id : ids) {
                      ackDAO.add(topicName, consumerId, id, DEFAULT_IP);
                    }
                  }
                }
              }
              return messageIDList;
            }
          }
        }
      }
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
    return null;
  }

}
