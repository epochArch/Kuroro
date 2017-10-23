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

import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorAsynHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DisruptorAsynHandler.class);
	private final Disruptor<WrappedMessageContainer> disruptor;
	private final RingBuffer<WrappedMessageContainer> ringBuffer;
	private final ProducerImpl producer;
	private final int sendTimes;
	private final DefaultPullStrategy defaultPullStrategy = new DefaultPullStrategy(
			500, 5 * 500);;
	private final ExecutorService es = Executors.newCachedThreadPool();

	@SuppressWarnings("unchecked")
	public DisruptorAsynHandler(ProducerImpl producerImpl) {
		this.producer = producerImpl;
		this.sendTimes = producer.getProducerConfig().getAsyncRetryTimes() == Integer.MAX_VALUE ? Integer.MAX_VALUE
				: producer.getProducerConfig().getAsyncRetryTimes() + 1;

		this.disruptor = new Disruptor<WrappedMessageContainer>(
				WrappedMessageContainer.EVENT_FACTORY, producerImpl
						.getProducerConfig().getAsyncQueueSize(), es);
		this.disruptor.handleEventsWith(messageEventHandler);
		this.ringBuffer = this.disruptor.start();
	}

	public void putMessage(Wrap msg) {
		try {
//			long next = ringBuffer.next();//Raise CPU 100% problem
			for (int i = 0; i < producer.getProducerConfig().getAsyncPutSpinCount() + 1; i++) {
				try {
					long next = ringBuffer.tryNext(1);
					WrappedMessageContainer messageEventContainer = ringBuffer
							.get(next);
					messageEventContainer.setWrappedMessage(msg);
					ringBuffer.publish(next);
					return;//publish successfully
				} catch (InsufficientCapacityException e) {
					continue;
				} catch (Exception e) {
					LOGGER.error("Exception on putMessage", e);
					throw e;//rethrow it
				}
			}
			//publish failed
			AsyncRejectionPolicy policy = producer.getAsyncRejectionPolicy();
			if (policy != null && msg instanceof WrappedMessage) {
				policy.onRejected(((WrappedMessage) msg).getContent(), producer);
			}else if (policy != null) {
				LOGGER.warn("Not a KuroroMessage: {}", msg);
			} else {
				LOGGER.error("Insufficient capacity when async send, discard it by default: {}", msg);
			}
		} catch (Exception e) {
			LOGGER.error("Buffersize is full,discard message");
		}
	}

	private EventHandler<WrappedMessageContainer> messageEventHandler = new EventHandler<WrappedMessageContainer>() {
		@Override
		public void onEvent(WrappedMessageContainer messageEventContainer,
				long arg1, boolean arg2) throws Exception {
			int leftRetryTimes = sendTimes;
			defaultPullStrategy.succeess();
			Wrap msgEvent = messageEventContainer.getWrappedMessage();
			for (leftRetryTimes = sendTimes; leftRetryTimes > 0;) {
				leftRetryTimes--;
				try {
					producer.getProducerService().sendMessage(msgEvent);
					messageEventContainer.setWrappedMessage(null);
				} catch (Exception e) {
					LOGGER.warn(e.getMessage(), e);
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
		}

	};

	public void shutdown() {
		this.disruptor.shutdown();
		if (!this.es.isShutdown())
			this.es.shutdown();

	}
}
