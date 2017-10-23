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

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.consumer.MessageFilter;

public class ConsumerConfig {
	private int threadPoolSize = 1;
	private MessageFilter messageFilter = MessageFilter.AllMatchFilter;
	private ConsumerType consumerType = ConsumerType.AUTO_ACKNOWLEDGE;
	private int minDelayOnException = 100; // ms
	private int maxDelayOnException = 3000; // ms
	private int retryCountOnException = 5; // 重试次数
	private int maxConnectionCount = 10;// 同一server Ip的同一主题的连接数
	private boolean idcProduct = false;
	private boolean isConsumeLocalZone = false;
	private MessageFilter consumeLocalZoneFilter = null;
	private ConsumerOffset consumerOffset;


	public int getMaxConnectionCount() {
		return maxConnectionCount;
	}

	public void setMaxConnectionCount(int maxConnectionCount) {
		this.maxConnectionCount = maxConnectionCount;
	}

	public int getThreadPoolSize() {
		return threadPoolSize;
	}

	/**
	 * 设置consumer处理消息的线程池线程数，默认为1
	 * 
	 * @param threadPoolSize
	 */
	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}

	/**
	 * 返回消息过滤方式
	 */
	public MessageFilter getMessageFilter() {
		return messageFilter;
	}

	/**
	 * 设置消息过滤方式
	 * 
	 * @param messageFilter
	 */
	public void setMessageFilter(MessageFilter messageFilter) {
		this.messageFilter = messageFilter;
	}

	/**
	 * Consumer的类型，包括2种类型：<br>
	 * 1.AUTO_ACKNOWLEDGE：尽量保证消息最多消费一次，不出现重复消费<br>
	 * 2.CLIENT_ACKNOWLEDGE：尽量保证消息最少消费一次，不出现消息丢失的情况<br>
	 */
	public ConsumerType getConsumerType() {
		return consumerType;
	}

	public void setConsumerType(ConsumerType consumerType) {
		this.consumerType = consumerType;
	}

	/**
	 * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
	 * 次重试之间最小的停顿时间 *
	 * <p>
	 * 默认值为100
	 * </p>
	 */

	public int getMinDelayOnException() {
		return minDelayOnException;
	}

	/**
	 * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
	 * 次重试之间最小的停顿时间 *
	 * <p>
	 * 默认值为100
	 * </p>
	 */
	public void setMinDelayOnException(int minDelayOnException) {
		this.minDelayOnException = minDelayOnException;
	}

	/**
	 * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
	 * 次重试之间最大的停顿时间
	 * <p>
	 * 默认值为3000
	 * </p>
	 */
	public int getMaxDelayOnException() {
		return maxDelayOnException;
	}

	/**
	 * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
	 * 次重试之间最大的停顿时间
	 * <p>
	 * 默认值为3000
	 * </p>
	 */
	public void setMaxDelayOnException(int maxDelayOnException) {
		this.maxDelayOnException = maxDelayOnException;
	}

	/**
	 * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，最多重试的次数
	 * <p>
	 * 默认值为5
	 * </p>
	 */
	public int getRetryCountOnException() {
		return retryCountOnException;
	}

	/**
	 * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，最多重试的次数
	 * <p>
	 * 默认值为5
	 * </p>
	 */

	public void setRetryCountOnException(int retryCountOnException) {
		this.retryCountOnException = retryCountOnException;
	}

	/**
	 *idc and zone product flag
	 * default zone product 
	 * */
	public boolean isIdcProduct(){
		return idcProduct;
	}

	public void setIdcProduct(boolean idcProduct) {
		this.idcProduct = idcProduct;
	}


	/*
	* local consume message of local zone produce message
	*
	* */
	public boolean isConsumeLocalZone(){
		return isConsumeLocalZone;
	}

	public void setIsConsumeLocalZone(boolean isConsumeLocalZone) {
		this.isConsumeLocalZone = isConsumeLocalZone;
	}

	/**
	 * 返回消息过滤方式
	 */
	public MessageFilter getConsumeLocalZoneFilter() {
		return consumeLocalZoneFilter;
	}

	/**
	 * 设置消息过滤方式
	 *
	 * @param consumeLocalZoneFilter
	 */
	public void setConsumeLocalZoneFilter(MessageFilter consumeLocalZoneFilter) {
		this.consumeLocalZoneFilter = consumeLocalZoneFilter;
	}

	public ConsumerOffset getConsumerOffset() {
		return consumerOffset;
	}

	public void setConsumerOffset(ConsumerOffset consumerOffset) {
		this.consumerOffset = consumerOffset;
	}

	@Override
	public String toString() {
		return String
				.format("ConsumerConfig [threadPoolSize=%s, messageFilter=%s, consumerType=%s, minDelayOnException=%s, maxDelayOnException=%s, retryCountOnException=%s, retryCountOnException=%s, idcProduct=%s, isConsumeLocalZone=%s, consumeLocalZoneFilter=%s, consumerOffset=%s]",
						threadPoolSize, messageFilter, consumerType,
						minDelayOnException, maxDelayOnException,
						retryCountOnException,retryCountOnException, idcProduct, isConsumeLocalZone, consumeLocalZoneFilter, consumerOffset);
	}
}
