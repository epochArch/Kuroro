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

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.consumer.ConsumerMessageType;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.ZipUtil;
import com.epocharch.kuroro.common.inner.wrap.WrappedConsumerMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedType;
import com.epocharch.kuroro.consumer.BackoutMessageException;
import com.epocharch.kuroro.consumer.NeedResendException;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.channel.Channel;
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

public class MessageClientHandler extends SimpleChannelUpstreamHandler {

	private static final Logger LOG = LoggerFactory.getLogger(MessageClientHandler.class);
	private final ConsumerImpl consumer;
	private ExecutorService service;
	private AtomicLong lastCheckTime = new AtomicLong();
	private volatile int consumerFlowControlMax = 0;
	private ChannelGroup channelGroup = new DefaultChannelGroup();
	private volatile boolean FLOWCONTROL_FLAG = true;
	private ZkClient zkClient = null;
	private int cycleTime = 1;
	private AtomicLong lastTime = new AtomicLong();
	private AtomicLong messageNumber = new AtomicLong();
	public final String SEND_EMAIL_NAME = "This Topic Started Flow Control!";
	private final int CONSUMER_FLOW_CONTROL_MAX = 6500;
	private final String localZone;
	private final String localIDC;
	private final String poolId;
	private String zoneZkRoot = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH);
	private String idcZkRoot = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);
	private final boolean isProduction = Boolean.parseBoolean(System.getProperty(InternalPropKey.POOL_LEVEL));

	public MessageClientHandler(ConsumerImpl consumer) {
		this.consumer = consumer;
		String topicPath = null;
		TopicConfigDataMeta tc = null ;

		if (isProduction) {
			topicPath = idcZkRoot + Constants.ZONE_TOPIC + Constants.SEPARATOR
					+ consumer.getDest().getName();
			zkClient = KuroroZkUtil.initIDCZk();
		} else {
			topicPath =  zoneZkRoot + Constants.ZONE_TOPIC + Constants.SEPARATOR
					+ consumer.getDest().getName();
			zkClient = KuroroZkUtil.initLocalMqZk();
		}

		try {
			try {
				tc = zkClient.readData(topicPath);
			}catch (Exception e) {
				for (int i = 0; i < 5; i++) {
					Thread.sleep(3000L);
					tc = zkClient.readData(topicPath);
					if (tc != null) {
						break;
					}
				}
			}finally {
				if (tc == null){
					tc = zkClient.readData(topicPath);
				}
			}
			if (tc != null) {
				if (KuroroUtil.isBlankObject(tc.isConsumerFlowControl())) {
					FLOWCONTROL_FLAG = true;
				}else {
					FLOWCONTROL_FLAG = tc.isConsumerFlowControl();
				}
				if (tc.getFlowConsumerIdMap() != null
						&& tc.getFlowConsumerIdMap().containsKey(consumer.getConsumerId())) {
					consumerFlowControlMax = tc.getFlowConsumerIdMap().get(consumer.getConsumerId());
				} else if (!KuroroUtil.isBlankObject(tc.getFlowSize())) {
					consumerFlowControlMax = tc.getFlowSize();
				}else{
					consumerFlowControlMax = CONSUMER_FLOW_CONTROL_MAX;
				}
				}
		} catch (Exception e) {
			LOG.error("init zk or read zk data is error!", e);
		}

		localIDC = KuroroZkUtil.localIDCName();
		localZone = KuroroZkUtil.localZone();
		poolId = KuroroUtil.poolIdName();
		if(KuroroUtil.isBlankString(poolId))
			LOG.warn("poolId is Null! please set current poolId!");

		observeTopicData(topicPath);
		service = Executors
				.newFixedThreadPool(consumer.getConfig().getThreadPoolSize(), new KuroroThreadFactory("Kuroro-consumer-client->"
						+ consumer.getDest().getName() + "-" + consumer.getConsumerId()));
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		WrappedConsumerMessage consumermessage = new WrappedConsumerMessage(ConsumerMessageType.WAKE, consumer.getConsumerId(),
				consumer.getDest(), consumer.getConfig().getConsumerType(), consumer.getConfig().getThreadPoolSize(), consumer.getConfig()
						.getMessageFilter(), localIDC, localZone, poolId, consumer.getConfig().getConsumeLocalZoneFilter(),
						consumer.getConfig().getConsumerOffset());
		e.getChannel().write(consumermessage);
		channelGroup.add(e.getChannel());
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		removeChannel(e);
		super.channelDisconnected(ctx, e);
		LOG.info("Channel(remoteAddress=" + e.getChannel().getRemoteAddress() + ") disconnected");
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
		// 记录收到消息，并且记录发来消息的server的地址

		if (LOG.isDebugEnabled()) {
			LOG.debug("MessageReceived from " +e.getChannel().getRemoteAddress()); 
		}
		 
		final WrappedMessage wrappedMessage = (WrappedMessage) e.getMessage();

		Boolean _isRestOffset = wrappedMessage.getRestOffset();
		if (_isRestOffset != null && consumer.getConfig().getConsumerOffset() != null) {
			Boolean consumerRestOffset = consumer.getConfig().getConsumerOffset().getRestOffset();
			if ((_isRestOffset.booleanValue() == true) && (consumerRestOffset.booleanValue() == false))
				consumer.getConfig().getConsumerOffset().setRestOffset(true);
		}

		if (wrappedMessage.getWrappedType().equals(WrappedType.HEARTBEAT)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Heartbeat received from {} of {}", e.getRemoteAddress(), this.consumer.getConsumerId() + "#"
						+ this.consumer.getDest().getName());
			}
			return;
		}
		Runnable task = new Runnable() {
			@Override
			public void run() {
				KuroroMessage kuroroMessage = wrappedMessage.getContent();
				Long messageId = kuroroMessage.getMessageId();
				WrappedConsumerMessage consumermessage = null;
				if (consumer.getConfig().getConsumerOffset() != null) {
					consumermessage = new WrappedConsumerMessage(ConsumerMessageType.ACK, messageId,
							consumer.isClosed(), consumer.getConfig().getConsumerOffset().getRestOffset());
				}else {
					consumermessage = new WrappedConsumerMessage(ConsumerMessageType.ACK, messageId,
							consumer.isClosed(), null);
				}
				// monitor

				// 处理消息
				// 如果是压缩后的消息，则进行解压缩
				try {
					if (kuroroMessage.getInternalProperties() != null) {
						if ("gzip".equals(kuroroMessage.getInternalProperties().get("compress"))) {
							kuroroMessage.setContent(ZipUtil.unzip(kuroroMessage.getContent()));
						}
					}
					try {

						DefaultPullStrategy pullStrategy = new DefaultPullStrategy(MessageClientHandler.this.consumer.getConfig()
								.getMinDelayOnException(), MessageClientHandler.this.consumer.getConfig().getMaxDelayOnException());
						int retryCount = 0;
						boolean success = false;
						while (!success && retryCount <= MessageClientHandler.this.consumer.getConfig().getRetryCountOnException()) {
							try {
								consumer.getListener().onMessage(kuroroMessage);
								success = true;
							} catch (BackoutMessageException e) {

								retryCount++;
								if (retryCount <= MessageClientHandler.this.consumer.getConfig().getRetryCountOnException()) {
									LOG.error("BackoutMessageException occur on onMessage(), onMessage() will be retryed soon [retryCount="
											+ retryCount + "]. ", e);
									pullStrategy.fail(true);
								} else {
									LOG.error("BackoutMessageException occur on onMessage(), onMessage() failed.", e);
								}
							} catch (NeedResendException e1) {
								if (consumer.getConfig().getConsumerType() != ConsumerType.CLIENT_ACKNOWLEDGE) {
									throw new IllegalStateException(
											"Can NOT throw NeedResendException wihthout a CLIENT_ACKNOWLEDGE consumer type");
								} else {
									consumermessage.setType(ConsumerMessageType.RESEND);
								}
							}
						}
					} catch (InterruptedException e) {
						LOG.warn("InterruptedException in MessageListener", e);
					} catch (Throwable throwable) {
						LOG.warn("Exception in MessageListener", throwable);
					}
				} catch (IOException e) {
					LOG.error("Can not uncompress message with messageId " + messageId, e);
				}

				try {
					if (e.getChannel().isOpen()) {
						e.getChannel().write(consumermessage);
					} else {
						LOG.info("e.getChannel...is close....");
					}
				} catch (RuntimeException e) {
					LOG.warn("Write to swallowC error.", e);
				}
			}
		};

		// flow control
		if (FLOWCONTROL_FLAG) {
			messageNumber.incrementAndGet();
			long _lastCheckTimeMillis = lastCheckTime.get();
			long _startTimeMillis = System.currentTimeMillis();
			if (messageNumber.get() >= consumerFlowControlMax) {
				try {
					service.submit(task);
					long _everyCostTime = (_startTimeMillis - lastTime.get()) / cycleTime;

					if (lastTime.get() >= 0 && (_everyCostTime <= Constants.CONSUMER_CHECK_CYCLElIFE) && _lastCheckTimeMillis == 0) {
						cycleTime++;
					} else if (_everyCostTime > Constants.CONSUMER_CHECK_CYCLElIFE && _lastCheckTimeMillis == 0) {
						cycleTime = 1;
					}

					if (cycleTime > 5) {
						lastCheckTime.compareAndSet(_lastCheckTimeMillis, _startTimeMillis);
						new Thread(new SendEmailThread(_startTimeMillis)).start();
						cycleTime = 1;
						LOG.warn("this topic " + consumer.getDest().getName() + "#" + consumer.getConsumerId() + "  starting flow control!");
					} else if (_lastCheckTimeMillis > 0 && _everyCostTime <= Constants.CONSUMER_CHECK_CYCLElIFE) {
						long _sleepTime = 0L;

						if (_startTimeMillis - lastTime.get() > Constants.CONSUMER_CHECK_CYCLElIFE) {
							LOG.warn("This topic(" + consumer.getDest().getName() + "#" + consumer.getConsumerId()
									+ ")  wait time more than consumer check cycleLife!...sleep 10ms ! ");
							_sleepTime = Constants.CONSUMER_CHECK_CYCLElIFE / 6;
						} else {
							_sleepTime = Constants.CONSUMER_CHECK_CYCLElIFE - _everyCostTime;
						}

						if ((_startTimeMillis - _lastCheckTimeMillis) > 25 * Constants.CONSUMER_CHECK_CYCLElIFE) {
							new Thread(new SendEmailThread(_startTimeMillis)).start();
							lastCheckTime.compareAndSet(_lastCheckTimeMillis, _startTimeMillis);
						}

						LOG.warn(consumer.getDest().getName() + "#" + consumer.getConsumerId() + "   starting sleeping " + _sleepTime
								+ " ms !");
						Thread.sleep(_sleepTime);
						LOG.warn(consumer.getDest().getName() + "#" + consumer.getConsumerId() + "    sleep is end!.....");
						_startTimeMillis = _startTimeMillis + _sleepTime;
					}

					messageNumber.set(0);
					lastTime.compareAndSet(lastTime.get(), _startTimeMillis);
				} catch (InterruptedException e1) {
					lastCheckTime.set(0);
					messageNumber.set(0);
					cycleTime = 1;
					LOG.error("This topic(" + consumer.getDest().getName() + "#" + consumer.getConsumerId()
							+ " )  control topic consumer is interrupted! ", e1);
				}
			} else {
				
				service.submit(task);
			}
		} else {
			if (lastCheckTime.get() > 0 || messageNumber.get() > 0 || cycleTime > 1) {
				lastCheckTime.set(0);
				messageNumber.set(0);
				cycleTime = 1;
			}
			service.submit(task);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// Close the connection when an exception is raised.
		removeChannel(e);
		Channel channel = e.getChannel();
		if (channel != null) {
			LOG.warn("Error from channel(remoteAddress=" + channel.getRemoteAddress() + ",switch broker server )", e.getCause());
			channel.close();
		}

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

	public ChannelGroup getChannelGroup() {
		return channelGroup;
	}

	public void observeTopicData(String topicPath) {
		if (!zkClient.exists(topicPath)) {
			LOG.error("topic is not in zk!...." + topicPath);
			throw new NullPointerException("topic is not in zk!..." + topicPath);
		}
		zkClient.subscribeDataChanges(topicPath, new IZkDataListener() {

			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
				TopicConfigDataMeta tc = (TopicConfigDataMeta) data;

				if(KuroroUtil.isBlankObject( tc.isConsumerFlowControl() ) ){
					FLOWCONTROL_FLAG = true;
					LOG.warn("FlowControl flag is null, set FlowControl is true...");
				} else {
					FLOWCONTROL_FLAG = tc.isConsumerFlowControl();
					LOG.warn(consumer.getDest().getName() + " metaData is change...FLOWCONTROL_FLAG: " + FLOWCONTROL_FLAG);
				}
				if ( tc.getFlowConsumerIdMap() != null
						&& tc.getFlowConsumerIdMap().containsKey(consumer.getConsumerId())) {
					consumerFlowControlMax = tc.getFlowConsumerIdMap().get(consumer.getConsumerId());
					LOG.warn(consumer.getDest().getName() + "#" + consumer.getConsumerId() + "  is change...consumerFlowControlMax: "
							+ consumerFlowControlMax);
				} else if (!KuroroUtil.isBlankObject(tc.getFlowSize())) {
					consumerFlowControlMax = tc.getFlowSize();
					LOG.warn(consumer.getDest().getName() + " metaData is change...consumerFlowControlMax: " + consumerFlowControlMax);
				}else {
					consumerFlowControlMax = CONSUMER_FLOW_CONTROL_MAX;
					LOG.warn(consumer.getDest().getName() + " get default consumerFLowControlMax value " + consumerFlowControlMax);
				}
			}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {

			}
		});
	}

	class SendEmailThread implements Runnable {
		long happenTime = 0L;

		public SendEmailThread(long happenTime) {
			this.happenTime = happenTime;
		}

		@Override
		public void run() {
			sendEmail(happenTime);
		}

		private void sendEmail(long time) {
			StringBuilder emailContent = new StringBuilder("topic name : ");
			emailContent.append(consumer.getDest().getName()).append(", consumerId : ").append(consumer.getConsumerId())
					.append(", consumer serverIp : ").append(consumer.getRemoteAddress()).append(", source consumerIp : ")
					.append(consumer.getConsumerIP()).append(", Exceeds the threshold > ")
					.append(consumerFlowControlMax + " !")
					.append(new Date(time - 5L * 60L * 1000L).toString());
			KuroroUtil.sendEmail(Constants.SEND_EMAIL_LIST, emailContent.toString(), SEND_EMAIL_NAME);
		}
	}

}
