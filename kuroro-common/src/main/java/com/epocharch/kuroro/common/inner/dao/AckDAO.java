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

public interface AckDAO {

  Long getMaxMessageID(String topicName, String consumerId);

  boolean isAcked(String topicName, String consumerId, Long messageId, String zone);

  boolean isAcked(String topicName, String consumerId, Long messageId, int index, String zone);

  Long getMaxMessageID(String topicName, String consumerId, int index, String zone);

  void add(String topicName, String consumerId, Long messageId, String sourceConsumerIp);

  void add(String topicName, String consumerId, Long messageId, String sourceConsumerIp, int index,
      String zone);

  int countBetween(String topic, String consumer, Long until, Long newTil);

  int countBetween(String topic, String consumer, Long until, Long newTil, int index, String zone);

  Long getMinMessageID(String topicName, String consumerId, String zone);

  Long getMinMessageID(String topicName, String consumerId, int index, String zone);

  void add(String topicName, String consumerId, Long messageId, String sourceConsumerIp, int index,
      String idc, String zone,
      String poolId);
}
