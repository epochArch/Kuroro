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

package com.epocharch.kuroro.common.inner.dao;

import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import java.util.List;
import org.bson.types.BSONTimestamp;

public interface MessageDAO {

  void saveMessage(String topicName, KuroroMessage kuroroMessage, int level);

  void saveMessage(String topicName, KuroroMessage kuroroMessage, int index, int level);

  void transferMessage(String topicName, KuroroMessage kuroroMessage, int level);

  void transferMessage(String topicName, KuroroMessage kuroroMessage, int index, int level);

  KuroroMessage getMessage(String topicName, Long messageId);

  KuroroMessage getMessage(String topicName, Long messageId, int index);

  List<KuroroMessage> getMessagesGreaterThan(String topicName, Long messageId, int size);

  List<KuroroMessage> getMessagesGreaterThan(String topicName, Long messageId, int size, int index);

  Long getMaxMessageId(String topicName);

  KuroroMessage getMaxMessage(String topicName);

  Long getMaxMessageId(String topicName, int index);

  KuroroMessage getMaxMessage(String topicName, int index);

  Long getMessageIDGreaterThan(String topicName, Long messageId, int size);

  Long getMessageIDGreaterThan(String topicName, Long messageId, int size, int index);

  List<KuroroMessage> getMessagesGreaterThan(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter);

  List<KuroroMessage> getMessagesGreaterThan(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter,
      int index, MessageFilter consumeLocalZoneFilter);

  List<Long> getMessagesIdGreaterThan(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter);

  List<Long> getMessagesIdGreaterThan(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter, int index);

  List<Long> getMessageIdBetween(String topic, Long min, Long max);

  List<Long> getMessageIdBetween(String topic, Long min, Long max, int index);

  Long getMinIdAfter(String topic, BSONTimestamp bsonTimestamp);

  Long getMinIdAfter(String topic, BSONTimestamp bsonTimestamp, int index);

  int countBetween(String topic, Long until, Long newTil);

  int countBetween(String topic, Long until, Long newTil, int index);

  Long getSkippedMessageId(String topic, Long until, int skip);

  Long getSkippedMessageId(String topic, Long until, int skip, int index);

  List<KuroroMessage> getMessagesBetween(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter, int index,
      MessageFilter consumeLocalZoneFilter);

  List<Long> getMessageIdBetween(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter, int index);

  List<KuroroMessage> getMessagesBetween(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter);

  List<Long> getMessageIdBetween(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter);

  Long getMinMessageId(String topicName, int index);
}
