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


package com.epocharch.kuroro.consumer.impl.inner;

import com.epocharch.kuroro.consumer.Consumer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread extends Thread {
	private static final Logger LOG = LoggerFactory
			.getLogger(ConsumerThread.class);

	private ClientBootstrap bootstrap;

	private InetSocketAddress remoteAddress;

	private long interval;

	private Consumer consumer;
 
	private final int retryMaxCount = 10;

	private AtomicBoolean isClosed = new AtomicBoolean(false);

	public AtomicBoolean getIsClosed() {
		return isClosed;
	}

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}

	public void setBootstrap(ClientBootstrap bootstrap) {
		this.bootstrap = bootstrap;
	}

	public void setRemoteAddress(InetSocketAddress remoteAddress) {
		this.remoteAddress = remoteAddress;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	@Override
	public void run() {
		int retryCount = 0;
		while (!Thread.currentThread().isInterrupted()) {
			synchronized (bootstrap) {
				if (!Thread.currentThread().isInterrupted()) {
					try {
						LOG.info("ConsumerThread-try connecting to "
								+ remoteAddress);
						while (remoteAddress == null) {// broker全部掉线，拿不到可用地址
							Thread.sleep(interval);
							LOG.warn("Maybe all brokers are down,waiting...");
							this.remoteAddress = this.consumer
									.getConsumerAddress();
						}
						ChannelFuture future = bootstrap.connect(remoteAddress);
						future.awaitUninterruptibly();// 等待连接创建成功
						LOG.debug("Connect finished");
						if (future.getChannel() != null && future.getChannel().isConnected()) {
							retryCount = 0;
							SocketAddress localAddress = future.getChannel()
									.getLocalAddress();
							LOG.info("ConsumerThread(localAddress="
									+ localAddress + ")-connected to "
									+ remoteAddress);
							// TODO:注册listener
							future.getChannel().getCloseFuture()
									.awaitUninterruptibly();// 等待channel关闭，否则一直阻塞！
							LOG.info("ConsumerThread(localAddress="
									+ localAddress + ")-closed from "
									+ remoteAddress);
						} else {
							retryCount++;
							if (retryCount >= retryMaxCount) {
								retryCount = 0;
								this.remoteAddress = this.consumer
										.getConsumerAddress();
							}
							LOG.debug("Retry {} times",retryCount);
						}
					} catch (Exception e) {
						LOG.error(e.getMessage(), e);
					}
				}
			}
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				isClosed.compareAndSet(false, true);
				Thread.currentThread().interrupt();
			}
		}
		LOG.info("ConsumerThread(remoteAddress=" + remoteAddress + ") done.");
	}

	
	
}
