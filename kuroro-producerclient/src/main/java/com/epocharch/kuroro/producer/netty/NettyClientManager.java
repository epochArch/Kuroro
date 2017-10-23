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

import com.epocharch.kuroro.common.netty.component.ClientManager;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.SimpleThreadPool;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

public class NettyClientManager implements ClientManager {

	private Executor bossExecutor;
	private Executor workerExecutor;
	private SimpleThreadPool clientResponseThreadPool;

	public NettyClientManager() {
	
		this.bossExecutor = Executors
				.newCachedThreadPool(new DefaultThreadFactory(
						"Producer-Client-BossExecutor"));
		this.workerExecutor = Executors
				.newCachedThreadPool(new DefaultThreadFactory(
						"Producer-Client-WorkerExecutor"));
		this.clientResponseThreadPool = new SimpleThreadPool(
				"Client-ResponseProcessor", 10, 100,
				new ArrayBlockingQueue<Runnable>(500),
				new ThreadPoolExecutor.CallerRunsPolicy());
		ThreadRenamingRunnable
				.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
	}
	
	@Override
	public Executor getBossExecutor() {
		return bossExecutor;
	}

	@Override
	public Executor getWorkerExecutor() {
		return workerExecutor;
	}

	/**
	 * @return the clientResponseThreadPool
	 */
	public SimpleThreadPool getClientResponseThreadPool() {
		return clientResponseThreadPool;
	}

	public void setClientResponseThreadPool(
			SimpleThreadPool clientResponseThreadPool) {
		this.clientResponseThreadPool = clientResponseThreadPool;
	}
}
