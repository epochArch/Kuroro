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

package com.epocharch.kuroro.common.netty.component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SimpleThreadPool {

  private String name;
  private ThreadPoolExecutor executor;
  private DefaultThreadFactory factory;

  protected SimpleThreadPool(String poolName) {
    this.name = poolName;
    this.executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool(new DefaultThreadFactory(poolName));
  }

  protected SimpleThreadPool(String poolName, int corePoolSize, int maximumPoolSize) {
    this.name = poolName;
    this.factory = new DefaultThreadFactory(poolName);
    this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        this.factory);
  }

  protected SimpleThreadPool(String poolName, int corePoolSize, int maximumPoolSize,
      BlockingQueue<Runnable> workQueue) {
    this.name = poolName;
    this.factory = new DefaultThreadFactory(poolName);
    this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
        workQueue, this.factory);
  }

  public SimpleThreadPool(String poolName, int corePoolSize, int maximumPoolSize,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {
    this.name = poolName;
    this.factory = new DefaultThreadFactory(poolName);
    this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
        workQueue, this.factory, handler);
  }

  public void execute(Runnable run) {
    this.executor.execute(run);
  }

  public <T> Future<T> submit(Callable<T> call) {
    return this.executor.submit(call);
  }

  @SuppressWarnings("rawtypes")
  public Future submit(Runnable run) {
    return this.executor.submit(run);
  }

  public ThreadPoolExecutor getExecutor() {
    return this.executor;
  }

  /**
   * @return the factory
   */
  public DefaultThreadFactory getFactory() {
    return factory;
  }
}
