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

package com.epocharch.kuroro.consumer;

import java.net.InetSocketAddress;

public interface Consumer {
 
    /**
     * 启动消费
     * <p/>
     * 所有consumer都初始化后调用
     */
	public void start();

    /**
     * 设置回调函数
     * @param messagelistener
     */
	public void setListener(MessageListener messagelistener);

    /**
     * 关闭consumer，及其子consumer
     */
	public void close();

    /**
     * 获取已连接的ConsumerServer的地址字符串表达形式
     * @return
     */
	public String getRemoteAddress();

    /**
     * 获取可连接的ConsumerServer地址
     * @return
     */
	public InetSocketAddress getConsumerAddress();

	public void restart();
	
	public void setBrokerGroupRotueManager(BrokerGroupRouteManager routeManager);
}
