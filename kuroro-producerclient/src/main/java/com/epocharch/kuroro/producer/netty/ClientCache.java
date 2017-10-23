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
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.netty.component.SimpleClient;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCache {

	private static final Logger LOG = LoggerFactory
			.getLogger(ClientCache.class);

	private Map<HostInfo, SimpleClient> cacheClients = new ConcurrentHashMap<HostInfo, SimpleClient>();

	public synchronized SimpleClient addConnect(HostInfo hi) {
		return addConnect(hi, cacheClients.get(hi));
	}

	public synchronized SimpleClient addConnect(HostInfo hi, SimpleClient client) {
		if (client == null) {
			client = new NettyClient(hi.getHost(), hi.getPort());
			this.cacheClients.put(hi, client);

		}
		try {
			if (!client.isConnected()) {
				client.connect();
			}

		} catch (NetException e) {

			LOG.error(e.getMessage(), e);
		}
		return client;
	}

	public synchronized void removeConnect(HostInfo hi) {
		this.cacheClients.get(hi).dispose();
		this.cacheClients.remove(hi);
	}

	public Map<HostInfo, SimpleClient> getCacheClients() {
		return cacheClients;
	}
}
