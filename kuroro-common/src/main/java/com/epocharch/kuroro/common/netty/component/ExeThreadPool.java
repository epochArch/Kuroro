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
import java.util.concurrent.RejectedExecutionHandler;

public class ExeThreadPool extends SimpleThreadPool {

  public ExeThreadPool(String poolName) {
    super(poolName);
  }

  public ExeThreadPool(String poolName, int corePoolSize, int maximumPoolSize) {
    super(poolName, corePoolSize, maximumPoolSize);
  }

  public ExeThreadPool(String poolName, int corePoolSize, int maximumPoolSize,
      BlockingQueue<Runnable> workQueue) {
    super(poolName, corePoolSize, maximumPoolSize, workQueue);
  }

  public ExeThreadPool(String poolName, int corePoolSize, int maximumPoolSize,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {
    super(poolName, corePoolSize, maximumPoolSize, workQueue, handler);
  }
}
