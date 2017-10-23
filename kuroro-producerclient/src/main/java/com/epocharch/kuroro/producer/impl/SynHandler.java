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

import com.epocharch.kuroro.common.inner.exceptions.SendFailedException;
import com.epocharch.kuroro.common.inner.producer.ProducerService;
import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.inner.wrap.WrappedProducerAck;
import com.epocharch.kuroro.producer.ProducerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynHandler implements ProducerHandler {

	private static final Logger logger = LoggerFactory.getLogger(SynHandler.class);
	private ProducerService producerService;
	private final int sendTimes;
	private final int DELAY_BASE_MULTI = 5;
	private final int SAVE_FAILED = 202;
	private final int delayBase = 500;
	private final int CHANNEL_CLOSE = 203;
	private final DefaultPullStrategy defaultPullStrategy = new DefaultPullStrategy(
			delayBase, DELAY_BASE_MULTI * delayBase);

	public SynHandler(ProducerImpl producerImpl) {
		this.sendTimes = producerImpl.getProducerConfig().getSyncRetryTimes() == Integer.MAX_VALUE ? Integer.MAX_VALUE
				: producerImpl.getProducerConfig().getSyncRetryTimes() + 1;
		producerImpl.getDestination();
		this.producerService = producerImpl.getProducerService();
	}

	@Override
	public Wrap sendWrappedMessage(Wrap msg) throws SendFailedException {
		if (msg == null) {
			throw new IllegalArgumentException("wrap should not be null.");
		}
		Wrap retMsg = null;
		for (int leftRetryTimes = sendTimes; leftRetryTimes > 0;) {
			defaultPullStrategy.succeess();
			leftRetryTimes--;
			try {
				msg.setACK(true);
				retMsg = producerService.sendMessage(msg);
				if (retMsg != null) {
					WrappedProducerAck wrappedProducerAck = (WrappedProducerAck) retMsg;
					if (wrappedProducerAck.getStatus() == CHANNEL_CLOSE) {
						continue;
					}
				}else {
					continue;
				}
			} catch (Exception e) {
				if (leftRetryTimes > 0) {
					try {
						defaultPullStrategy.fail(true);
					} catch (InterruptedException e1) {
						break;
					}
					continue;

				} else {
					// 记录hedwig-monitor
					throw new SendFailedException("Message sent failed", e);
				}
			}
			// fix:当远程mongodb保存失败，返回失败状态码，不重试问题。
			boolean needRetry = false;
			if (retMsg != null && retMsg instanceof WrappedProducerAck) {
				WrappedProducerAck ack = (WrappedProducerAck) retMsg;
				if (ack != null && ack.getStatus() == SAVE_FAILED) {
					needRetry = true;
				}
			}
			if (needRetry == false) {
				break;
			}
		}
		return retMsg;
	}

	@Override
	public void shutdown() {

	}

}
