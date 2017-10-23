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

import com.epocharch.kuroro.common.ProtocolType;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.exceptions.SendFailedException;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.producer.ProducerService;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.TimeUtil;
import com.epocharch.kuroro.common.inner.util.ZipUtil;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedProducerAck;
import com.epocharch.kuroro.common.message.Destination;
import com.epocharch.kuroro.producer.Producer;
import com.epocharch.kuroro.producer.ProducerConfig;
import com.epocharch.kuroro.producer.ProducerHandler;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl implements Producer {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ProducerImpl.class);

	private final Destination destination; // Producer消息目的
	private final ProducerConfig producerConfig = new ProducerConfig();
	private final ProducerService producerService;
	private final ProducerHandler producerHandler;
	private AsyncRejectionPolicy asyncRejectionPolicy;
	private ZkClient _zkClient = null;
	private final String localZone;
	private final String poolId;
	private String zoneKuroroZkPath;
	private boolean idcProdction = Boolean.parseBoolean(System.getProperty(InternalPropKey.POOL_LEVEL));

	public ProducerImpl(Destination destination, ProducerConfig producerConfig,
			ProducerService producerService) {
		if (producerConfig != null) {
			this.producerConfig.setAsyncRetryTimes(producerConfig
					.getAsyncRetryTimes());
			this.producerConfig.setMode(producerConfig.getMode());
			this.producerConfig.setResumeLastSession(producerConfig
					.isResumeLastSession());
			this.producerConfig.setSyncRetryTimes(producerConfig
					.getSyncRetryTimes());
			this.producerConfig.setThreadPoolSize(producerConfig
					.getThreadPoolSize());
			this.producerConfig.setZipped(producerConfig.isZipped());
			this.producerConfig.setAsyncQueueSize(producerConfig
					.getAsyncQueueSize());
			this.producerConfig.setHessianCompressionThreshold(producerConfig
					.getHessianCompressionThreshold());
		} else {
			LOGGER.warn("config is null, use default settings.");
		}
		this.destination = destination;
		this.producerService = producerService;
		localZone = KuroroZkUtil.localZone();
		poolId = KuroroUtil.poolIdName();
		if (KuroroUtil.isBlankString(poolId)) {
			LOGGER.warn("=== poolId is not null! ===");
		}

		//listener topic  sendFlag of topicMeta changed status
		if (idcProdction) {
			String idcZkRoot = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);
			String idcZkTopicPath = idcZkRoot + Constants.ZONE_TOPIC + Constants.SEPARATOR + destination.getName();
			_zkClient = KuroroZkUtil.initLocalMqZk();
			observeSendFlagChanged(idcZkTopicPath);
		} else {
			try {
				_zkClient = KuroroZkUtil.initLocalMqZk();
				zoneKuroroZkPath = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH);
				String zoneTopicPath = zoneKuroroZkPath + Constants.ZONE_TOPIC + Constants.SEPARATOR
						+ destination.getName();
				if (_zkClient.exists(zoneTopicPath)) {
					observeSendFlagChanged(zoneTopicPath);
				}
			} catch (Exception e) {
				LOGGER.error((new StringBuilder()).append("Error producerImpl occour from ZK :")
						.append(e.getMessage()).toString(), e);
			}
	    }
		
		switch (this.producerConfig.getMode()) {
		case SYNC_MODE:
			producerHandler = new SynHandler(this);
			break;
		case ASYNC_MODE:
			producerHandler = new AsynHandler(this);
			break;
		default:
			producerHandler = new SynHandler(this);
			break;
		}

	}

	@Override
	public String sendMessage(Object content) throws SendFailedException {
		return sendMessage(content, null, null, ProtocolType.HESSIAN);
	}

	/**
	 * 将Object类型的content发送到指定的Destination
	 *
	 * @param content
	 *            待发送的消息内容
	 * @return 异步模式返回null，同步模式返回将content转化为json字符串后，与其对应的SHA-1签名
	 * @throws SendFailedException
	 *             发送失败则抛出此异常
	 */
	@Override
	public String sendMessage(Object content, ProtocolType protocolType)
			throws SendFailedException {
		return sendMessage(content, null, null, protocolType);
	}

	@Override
	public String sendMessage(Object content, String messageType,
                              ProtocolType protocolType) throws SendFailedException {
		return sendMessage(content, null, messageType, protocolType);
	}

	/**
	 * 将Object类型的content发送到指定的Destination
	 *
	 * @param content
	 *            待发送的消息内容
	 * @param properties
	 *            消息属性，留作后用
	 * @return 异步模式返回null，同步模式返回content的SHA-1字符串
	 * @throws SendFailedException
	 *             发送失败则抛出此异常
	 */
	@Override
	public String sendMessage(Object content, Map<String, String> properties,
                              ProtocolType protocolType) throws SendFailedException {
		return sendMessage(content, properties, null, protocolType);
	}

	@Override
	public String sendMessage(Object content, Map<String, String> properties,
                              String messageType, ProtocolType protocolType)
			throws SendFailedException {
		return sendMessage(content, properties, messageType, protocolType, true);
	}

	private String sendMessage(Object content, Map<String, String> properties,
                               String messageType, ProtocolType protocolType, Boolean isProducerAck)
			throws SendFailedException {
		if(producerConfig.isSendFlag() ){
		if (content == null) {
			throw new IllegalArgumentException(
					"Message content can not be null.");
		}

		KuroroMessage kuroroMessage = new KuroroMessage();
		kuroroMessage.setGeneratedTime(new Date());
		kuroroMessage.setCreatedTime(TimeUtil.getNowTime());
		kuroroMessage.setZone(localZone);
		if (!KuroroUtil.isBlankString(poolId))
			kuroroMessage.setPoolId(poolId);

		if (protocolType == null) {
			kuroroMessage.setProtocolType(ProtocolType.HESSIAN.toString());
		} else {
			kuroroMessage.setProtocolType(protocolType.toString());
		}
		String ret = null;
		Map<String, String> zipProperties = null;

		kuroroMessage.setContentFromSend(content,
				this.producerConfig.getHessianCompressionThreshold());

		if (messageType != null) {
			kuroroMessage.setType(messageType);
		}
		if (properties != null) {
			for (Map.Entry<String, String> entry : properties.entrySet()) {
				if (!(entry.getKey() instanceof String)
						|| (entry.getValue() != null && !(entry.getValue() instanceof String))) {
					throw new IllegalArgumentException(
							"Type of properties should be Map<String, String>.");
				}
			}
			kuroroMessage.setProperties(properties);
		}
		if (producerConfig.isZipped()) {
			zipProperties = new HashMap<String, String>();
			try {
				kuroroMessage
						.setContent(ZipUtil.zip(kuroroMessage.getContent()));
				zipProperties.put("compress", "gzip");
			} catch (Exception e) {
				LOGGER.warn(
						"Compress message failed.Content="
								+ kuroroMessage.getContent(), e);
				zipProperties.put("compress", "failed");
			}
			kuroroMessage.setInternalProperties(zipProperties);
		}
		// 注册监控ID
		String eventID = null;
		WrappedMessage wrappedMsg = new WrappedMessage(destination,
				kuroroMessage);
		wrappedMsg.setMonitorEventID(eventID);
		wrappedMsg.setACK(isProducerAck);
		switch (producerConfig.getMode()) {
		case SYNC_MODE:// 同步模式
			WrappedProducerAck msgAck = (WrappedProducerAck) producerHandler
					.sendWrappedMessage(wrappedMsg);
			if (msgAck != null && msgAck.getStatus() == 200) {// 此处硬编码出于broker没有提取相关常量的考虑
				ret = msgAck.getShaInfo();
			} else if (msgAck != null) {
				throw new SendFailedException(msgAck.getShaInfo());
			} else {
				throw new RuntimeException("Send message error");
			}
			break;
		case ASYNC_MODE:// 异步模式
			producerHandler.sendWrappedMessage(wrappedMsg);
			break;
		}
		return ret;
	}else{
		return "broker stop client send message.....";
	}
	}

	public ProducerConfig getProducerConfig() {
		return producerConfig;
	}

	/**
	 * @return 返回producer消息目的地
	 */
	public Destination getDestination() {
		return destination;
	}

	public ProducerService getProducerService() {
		return producerService;
	}

	@Override
	public String forwardMessage(KuroroMessage message, Boolean isProducerAck)
			throws SendFailedException {
		String ret = null;
		WrappedMessage wrappedMsg = new WrappedMessage(destination, message);
		// wrappedMsg.setMonitorEventID(eventID);
		wrappedMsg.setACK(isProducerAck);
		switch (producerConfig.getMode()) {
		case SYNC_MODE:// 同步模式
			WrappedProducerAck msgAck = (WrappedProducerAck) producerHandler
					.sendWrappedMessage(wrappedMsg);
			if (msgAck != null) {
				ret = msgAck.getShaInfo();
			}
			break;
		case ASYNC_MODE:// 异步模式
			producerHandler.sendWrappedMessage(wrappedMsg);
			break;
		}
		return ret;
	}

	@Override
	public void setAsyncRejectionPolicy(AsyncRejectionPolicy asyncRejectionPolicy) {
		if (asyncRejectionPolicy == null) {
			throw new NullPointerException("AsyncRejectionPolicy");
		}
		this.asyncRejectionPolicy = asyncRejectionPolicy;
	}

	public AsyncRejectionPolicy getAsyncRejectionPolicy() {
		return asyncRejectionPolicy;
	}
	
	private void observeSendFlagChanged(String basePath){
		_zkClient.subscribeDataChanges(basePath, new IZkDataListener() {

			@Override
			public void handleDataChange(String dataPath, Object data)
					throws Exception {
				if(data != null){
					TopicConfigDataMeta topicConfigData = (TopicConfigDataMeta) data;
					if(topicConfigData.isSendFlag()!=null){
						producerConfig.setSendFlag(topicConfigData.isSendFlag());
					}else{
						producerConfig.setSendFlag(true);
						LOGGER.info("topicConfigData.....sendFlag  is null");
						}
				}
			}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				
			}
			
		});
	}

	@Override
	public void close() {
		producerHandler.shutdown();
	}
}
