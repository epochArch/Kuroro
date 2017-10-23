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


import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.netty.component.RouteManager;
import com.epocharch.kuroro.common.netty.component.SimpleClient;
import java.util.Collection;

public class BrokerGroupRouteManager {
	
	private int maxRetryTimes = 5;
	private ClientCache clientCache;
	private RouteManager routeManager;
	
	/**
	 * ctor
	 * @param destZkPath	topic zk path
	 */
	public BrokerGroupRouteManager(String destZkPath){
		this.clientCache = new ClientCache();
		this.routeManager = new RouteManager(destZkPath);
	}
	
	public SimpleClient getSimpleClient() {
		HostInfo hi = this.routeManager.route();
		if (hi == null) {
			throw new NullPointerException("can not fund MQ Producer Server! please contact MQ Owner！");
		}
		SimpleClient client = select(hi);
		int retry = 0;
		while (!client.isConnected()) {
			if (retry++ > maxRetryTimes) {
				return null;
			}
			hi = routeManager.route();
			client = select(hi);
		}
		return client;
	}

	private SimpleClient select(HostInfo hi) {
		SimpleClient client = clientCache.getCacheClients().get(hi);
		if (client == null) {
			synchronized (clientCache) {
				if ((client = clientCache.getCacheClients().get(hi)) == null) {
					client = clientCache.addConnect(hi, client);
				}
			}

		}

		if (!client.isConnected()) {
			client.connect();
		}

		return client;
	}
	
	/**
	 * close all connection of current routeManager
	 */
	public void closeAllConnection(){
		Collection<HostInfo> hostInfos = this.routeManager.getAllHostInfo();
		for(HostInfo hi : hostInfos){
			SimpleClient client = clientCache.getCacheClients().get(hi);
			
			if (client != null && client.isConnected()) {
				client.close();
			}
			clientCache.removeConnect(hi);
		}
	}
	
	public Collection<HostInfo> getHostInfos() {
		return this.routeManager.getAllHostInfo();
	}
}
