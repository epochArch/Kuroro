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

package com.epocharch.kuroro.consumer;

import com.epocharch.kuroro.common.message.Destination;
import java.util.List;

public interface ConsumerFactory {

	/**
	 * 创建带特殊要求（ConsumerConfig）的consumer
	 * 
	 * @param dest
	 *            消息目的地，类型为{@link Destination}
	 * @param consumerId
	 * @param config
	 *            consumer配置信息
	 * @return
	 */
	Consumer createConsumer(Destination dest, String consumerId,
                            ConsumerConfig config);

	/**
	 * 创建纯本地的Consumer，不消费异地消息
	 * @param dest
	 * @param consumerId
	 * @param config
	 * @return
	 */
	public Consumer createLocalConsumer(Destination dest, String consumerId, ConsumerConfig config);

	/**
	 * 创建异地消息消费者，指定zoneNames
	 * @param dest
	 * @param consumerId
	 * @param config
	 * @return
	 */
	public List<Consumer> createTransferredConsumer(Destination dest, String consumerId, ConsumerConfig config);

	/**
	 * 创建普通的consumer
	 * 
	 * @param dest
	 *            消息目的地，类型为{@link Destination}
	 * @param consumerId
	 * @return
	 */
	Consumer createConsumer(Destination dest, String consumerId);

	Consumer createConsumer(Destination dest);
}
