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

package com.epocharch.kuroro.producer.impl;


import com.epocharch.kuroro.common.inner.exceptions.MemeoryQueueException;
import com.epocharch.kuroro.common.inner.exceptions.SendFailedException;
import com.epocharch.kuroro.common.inner.producer.ProducerService;
import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.memeory.MemeoryQueue;
import com.epocharch.kuroro.common.memeory.MemeoryQueueConfig;
import com.epocharch.kuroro.common.memeory.MemeoryQueueImpl;
import com.epocharch.kuroro.producer.ProducerHandler;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AsynHandler implements ProducerHandler {

	private static final KuroroThreadFactory threadFactory = new KuroroThreadFactory();
	private static Map<String, MemeoryQueue<Wrap>> messageQueues = new ConcurrentHashMap<String, MemeoryQueue<Wrap>>();
	private static Map<String, DisruptorAsynHandler> disruptorQueues = new ConcurrentHashMap<String, DisruptorAsynHandler>();
	private final ProducerImpl producer;
	private final MemeoryQueue<Wrap> messageQue;
	private final DisruptorAsynHandler disruptorAsynHandler;

	public AsynHandler(ProducerImpl producerImpl) {
		this.producer = producerImpl;
		boolean isResume = producerImpl.getProducerConfig().isResumeLastSession();
		if (isResume) {
			this.disruptorAsynHandler = null;
			this.messageQue = getMessageQueue(producerImpl.getDestination().getName(), isResume, producerImpl.getProducerConfig()
					.getAsyncQueueSize());
			this.start();
		} else {
			this.messageQue = null;
			this.disruptorAsynHandler = getDisruptor(producerImpl.getDestination().getName(), producerImpl);
		}
	}

	private synchronized static DisruptorAsynHandler getDisruptor(String topicName, ProducerImpl producerImpl) {
		if (disruptorQueues.containsKey(topicName)) {
			return disruptorQueues.get(topicName);
		}
		DisruptorAsynHandler disruptor = new DisruptorAsynHandler(producerImpl);
		disruptorQueues.put(topicName, disruptor);
		return disruptor;
	}

	private synchronized static MemeoryQueue<Wrap> getMessageQueue(String topicName, boolean isNeedResume, int memeorySize) {
		if (messageQueues.containsKey(topicName)) {
			return messageQueues.get(topicName);
		}
		MemeoryQueueConfig config = new MemeoryQueueConfig();
		config.setMemeoryMaxSize(memeorySize);
		config.setNeedResume(isNeedResume);
		MemeoryQueue<Wrap> msgQue = new MemeoryQueueImpl<Wrap>(config, topicName);
		messageQueues.put(topicName, msgQue);
		return messageQueues.get(topicName);
	}

	@Override
	public Wrap sendWrappedMessage(Wrap msg) throws SendFailedException {
		msg.setACK(false);
		try {
			if (this.producer.getProducerConfig().isResumeLastSession()) {
				messageQue.add(msg);
			} else {
				this.disruptorAsynHandler.putMessage(msg);
			}
		} catch (MemeoryQueueException e) {
			throw new SendFailedException("Add message to ,memeoryqueue failed.", e);
		}
		return null;
	}

	// 启动处理线程
	private void start() {
		int threadPoolSize = producer.getProducerConfig().getThreadPoolSize();
		for (int idx = 0; idx < threadPoolSize; idx++) {
			Thread t = threadFactory.newThread(new SendMsgTask(), "kuroro-AsyncProducer-");
			t.setDaemon(true);
			t.start();
		}
	}

	private class SendMsgTask implements Runnable {

		private final int sendTimes = producer.getProducerConfig().getAsyncRetryTimes() == Integer.MAX_VALUE ? Integer.MAX_VALUE : producer
				.getProducerConfig().getAsyncRetryTimes() + 1;
		private int leftRetryTimes = sendTimes;
		private final int DELAY_BASE_MULTI = 5;
		private final int delayBase = 500;
		private final ProducerService producerService = producer.getProducerService();
		private Wrap msg = null;

		@Override
		public void run() {
			DefaultPullStrategy defaultPullStrategy = new DefaultPullStrategy(delayBase, DELAY_BASE_MULTI * delayBase);
			while (true) {
				try {
					defaultPullStrategy.succeess();
					msg = messageQue.get();
					for (leftRetryTimes = sendTimes; leftRetryTimes > 0;) {
						leftRetryTimes--;
						try {
							producerService.sendMessage(msg);
						} catch (Throwable e) {
							e.printStackTrace();
							if (leftRetryTimes > 0) {
								try {
									defaultPullStrategy.fail(true);
								} catch (InterruptedException e1) {
									return;
								}
								continue;
							}

						}
						break;
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}

	}

	@Override
	public void shutdown() {
		if (this.disruptorAsynHandler != null)
			this.disruptorAsynHandler.shutdown();
	}

}
