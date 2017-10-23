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

package com.epocharch.kuroro.producer.netty;

import com.epocharch.kuroro.common.inner.exceptions.NetException;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.netty.component.CallbackFuture;
import com.epocharch.kuroro.common.netty.component.ExeThreadPool;
import com.epocharch.kuroro.common.netty.component.Invoker;
import com.epocharch.kuroro.common.netty.component.SimpleCallback;
import com.epocharch.kuroro.common.netty.component.SimpleClient;
import com.epocharch.kuroro.common.netty.component.SimpleFuture;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultInvoker implements Invoker {
	private static final Logger LOG = LoggerFactory
			.getLogger(DefaultInvoker.class);
	private AtomicLong sequenceMaker = new AtomicLong(0);
	private final Long DEFAULT_TIMEOUT = 5000L;
	private ExeThreadPool threadPool = new ExeThreadPool(
			"Kuroro-ThreadPool-Invoker-Exe");
	private ExeThreadPool cycThreadPool = new ExeThreadPool(
			"Kuroro-ThreadPool-Cyc-Exe");
	private RequestMap requestMap = new RequestMap();
	private static Invoker invoker;

	private DefaultInvoker() {
		cycThreadPool.execute(new TimeoutCheck());
	}

	private synchronized static void createInvoker() {
		if (invoker != null) {
			return;
		}
		invoker = new DefaultInvoker();
		BrokerGroupRouteManagerFactory.getInstacne().setInvoker(invoker);
	}

	public static Invoker getInstance() {
		if (invoker == null) {
			createInvoker();
		}
		return invoker;
	}

	public void invokeCallback(Wrap msg, SimpleCallback callBack)
			throws NetException {
		
		WrappedMessage wrappedMessage = (WrappedMessage)msg;
		
		BrokerGroupRouteManager producerRouteManager 
				= BrokerGroupRouteManagerFactory.getInstacne().getBrokerGroupRouteManagerByDestination(wrappedMessage.getDestination().getName());
		
		SimpleClient client = producerRouteManager.getSimpleClient();
		if (client == null) {
			throw new NetException("Connected broker not found");
		}
		msg.setCreatedMillisTime(System.currentTimeMillis());
		long seq = sequenceMaker.incrementAndGet();
		msg.setSequence(seq);
		if (msg.isACK()) {
			Object[] callData = new Object[2];
			int index = 0;
			callData[index++] = msg;
			callData[index++] = callBack;
			this.requestMap.putData(seq, callData);
			this.requestMap.putProducerRouteManager(seq, producerRouteManager);
		}
		client.write(msg, callBack);

	}

	public SimpleFuture invokeFuture(Wrap msg) {
		CallbackFuture future = new CallbackFuture();
		invokeCallback(msg, future);
		return future;
	}

	public Wrap invokeSync(Wrap msg) throws NetException, InterruptedException {
		SimpleFuture future = invokeFuture(msg);
		Wrap res = future.get(DEFAULT_TIMEOUT);
		;
		return res;
	}

	public void invokeAck(Wrap ackMsg) {
		Object[] callData = requestMap.getData(ackMsg.getSequence());
		if (callData != null) {
			SimpleCallback callback = (SimpleCallback) callData[1];
			callback.callback(ackMsg);
			this.threadPool.execute(callback);
			this.requestMap.remove(ackMsg.getSequence());
		} else {
			LOG.warn("no producer for ackMsg:" + ackMsg.getSequence());
		}

	}

	private class TimeoutCheck implements Runnable {
		public void run() {
			while (true) {
				try {
					long now = System.currentTimeMillis();
					for (Long key : requestMap.getInnerMap().keySet()) {
						Object[] requestData = requestMap.getData(key);
						if (requestData != null) {
							Wrap requestMsg = (Wrap) requestData[0];
							// 超时移除
							if (requestMsg.getCreatedMillisTime()
									+ DEFAULT_TIMEOUT < now) {
								requestMap.remove(key);
								LOG.warn("remove timeout key:" + key);
							}
						}
					}
					Thread.sleep(1000);
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
			}

		}

	}

	private class RequestMap{
		private Map<Long, Object[]> requestMap = new ConcurrentHashMap<Long, Object[]>();
		
		public Map<Long, Object[]> getInnerMap(){
			return requestMap;
		}
		
		public Object[] getData(Long ackSequence){
			return requestMap.get(ackSequence);
		}
		public void putData(Long ackSequence, Object[] data){
			requestMap.put(ackSequence, data);
		}
		public void putProducerRouteManager(Long ackSequence, BrokerGroupRouteManager producerRouteManager){
			BrokerGroupRouteManagerFactory.getInstacne().addBrokerGroupRouteManagers(ackSequence, producerRouteManager);
		}
		public void remove(Long ackSequence){
			requestMap.remove(ackSequence);
			BrokerGroupRouteManagerFactory.getInstacne().removeBrokerGroupRouteManager(ackSequence);
		}
	}
}
