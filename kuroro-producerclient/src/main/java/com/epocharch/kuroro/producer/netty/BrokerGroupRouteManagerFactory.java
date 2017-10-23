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

import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.netty.component.Invoker;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 针对每个brokerGroup的routeManager工厂
 * @author chezhihong
 * @version 1.0  2015-8-20 下午02:53:10
 * @since 1.0
 */
public class BrokerGroupRouteManagerFactory {
	
	private BrokerGroupRouteManagerFactory(){}
	private static BrokerGroupRouteManagerFactory brokerGroupRouteManagerFactory;
	public static BrokerGroupRouteManagerFactory getInstacne() {
		if (brokerGroupRouteManagerFactory == null) {
			synchronized (BrokerGroupRouteManagerFactory.class) {
				if (brokerGroupRouteManagerFactory == null) {
					brokerGroupRouteManagerFactory = new BrokerGroupRouteManagerFactory();
				}
			}
		}
		return brokerGroupRouteManagerFactory;
	}
	
	/**
	 * brokerGroup and BrokerGroupRouteManager map
	 */
	private static ConcurrentHashMap<String, BrokerGroupRouteManager> brokerGroupRouteManagers
							= new ConcurrentHashMap<String, BrokerGroupRouteManager>();
	/**
	 * destination and BrokerGroupRouteManager map
	 */
	private static Map<String, BrokerGroupRouteManager> destinationRouteManagers
							= new ConcurrentHashMap<String, BrokerGroupRouteManager>();
	/**
	 * ack msg's sequenece and brokerGroupRouteManager map
	 */
	private static ConcurrentHashMap<Long, BrokerGroupRouteManager> ackSequenceBrokerGroupRouteManagers
							= new ConcurrentHashMap<Long, BrokerGroupRouteManager>();
	/**
	 * 本地仅允许group与topic唯一
	 * 用于判断在系统切换topic的brokerGroup属性时是否需要关闭旧的routeManager
	 */
	private static Map<String, Vector<String>> brokerGroupDestinations = new ConcurrentHashMap<String, Vector<String>>();
	
	
	private static Invoker invoker;
	
	public BrokerGroupRouteManager createBrokerGroupRouteManager(String brokerGroup, String destination, String destZkPath){
		addIfNotExist(brokerGroup, destination);
		
		BrokerGroupRouteManager brokerGroupRouteManager = null;
		if(brokerGroupRouteManagers.containsKey(brokerGroup)){
			brokerGroupRouteManager = brokerGroupRouteManagers.get(brokerGroup);
		}else{
			brokerGroupRouteManager = new BrokerGroupRouteManager(destZkPath);
			brokerGroupRouteManagers.put(brokerGroup, brokerGroupRouteManager);
		}
		destinationRouteManagers.put(destination, brokerGroupRouteManager);
		return brokerGroupRouteManager;
	}
	
	public BrokerGroupRouteManager getBrokerGroupRouteManager(String brokerGroup){
		return brokerGroupRouteManagers.get(brokerGroup);
	}
	public BrokerGroupRouteManager getBrokerGroupRouteManagerByDestination(String destination){
		return destinationRouteManagers.get(destination);
	}
	public BrokerGroupRouteManager getBrokerGroupRouteManager(Long ackSequence){
		return ackSequenceBrokerGroupRouteManagers.get(ackSequence);
	}
	public void addBrokerGroupRouteManagers(Long ackSequence, BrokerGroupRouteManager brokerGroupRouteManager){
		ackSequenceBrokerGroupRouteManagers.put(ackSequence, brokerGroupRouteManager);
	}
	public void removeBrokerGroupRouteManager(String brokerGroup){
		brokerGroupRouteManagers.remove(brokerGroup);
	}
	public void removeBrokerGroupRouteManager(Long ackSequence){
		ackSequenceBrokerGroupRouteManagers.remove(ackSequence);
	}
	
	public void setInvoker(Invoker invoker) {
		BrokerGroupRouteManagerFactory.invoker = invoker;
	}

	public void processAckMsg(Wrap ackMsg) {
		BrokerGroupRouteManagerFactory.invoker.invokeAck(ackMsg);
	}	
	
	/**
	 * 绑定destination到brokerGroup，同时删除其他brokerGroup与此destination的绑定关系，
	 * 删除后的brokerGroup如果没有与任何destination绑定，则从map删除并关闭其routeManager的所有连接
	 */
	public void bandingDestinationToBrokerGroup(String brokerGroup, String destination){
		addIfNotExist(brokerGroup, destination);
		
		Iterator<Map.Entry<String, Vector<String>>> it = brokerGroupDestinations.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, Vector<String>> entry= it.next();
            String key = entry.getKey();
            if(key.equalsIgnoreCase(brokerGroup)) continue;
            
            Vector<String> dests1 = entry.getValue();
            Iterator<String> it1 = dests1.iterator();
            while (it1.hasNext()) {
            	String tempDest = it1.next();
            	if (tempDest.equalsIgnoreCase(destination)) {
            		it1.remove();
            	}
            }
            
            if(dests1.size() <= 0){
            	it.remove();
            	removeBrokerGroupAndCloseConnection(key);
            }
		}
	}
	
	/**
	 * add destination to destination list of brokerGroup
	 * @param brokerGroup
	 * @param destination
	 * @return 
	 */
	private synchronized void addIfNotExist(String brokerGroup, String destination){
		Vector<String> dests = brokerGroupDestinations.get(brokerGroup);
		if(null == dests){
			dests = new Vector<String>();
		}
		boolean isAdded = false;
		for(String dest: dests){
			if(dest.equalsIgnoreCase(destination)){
				isAdded = true;
			}
		}
		if(!isAdded){
			dests.add(destination);
			brokerGroupDestinations.put(brokerGroup, dests);
		}
	}
	
	/**
	 * remove brokerGroup from brokerGroupPouteManagers, and close connection of its RouteManager
	 * @param brokerGroup
	 */
	private void removeBrokerGroupAndCloseConnection(String brokerGroup){
		//close connection
		BrokerGroupRouteManager brokerGroupRouteManager = getBrokerGroupRouteManager(brokerGroup);
		brokerGroupRouteManager.closeAllConnection();
		
		//remove brokerGroupRouteManager
		removeBrokerGroupRouteManager(brokerGroup);
	}
}
