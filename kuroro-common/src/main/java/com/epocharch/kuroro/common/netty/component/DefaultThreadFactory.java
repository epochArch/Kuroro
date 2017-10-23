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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultThreadFactory implements ThreadFactory {

  static final AtomicInteger poolNumber = new AtomicInteger(1);
  final AtomicInteger threadNumber;
  final ThreadGroup group;
  final String namePrefix;
  final boolean isDaemon;

  public DefaultThreadFactory() {
    this("Default-Pool");
  }

  public DefaultThreadFactory(String name) {
    this(name, true);
  }

  public DefaultThreadFactory(String preffix, boolean daemon) {
    this.threadNumber = new AtomicInteger(1);

    this.group = new ThreadGroup(preffix + "-" + poolNumber.getAndIncrement() + "-threadGroup");

    this.namePrefix = preffix + "-" + poolNumber.getAndIncrement() + "-thread-";
    this.isDaemon = daemon;
  }

  public Thread newThread(Runnable r) {
    Thread t = new Thread(this.group, r, this.namePrefix + this.threadNumber.getAndIncrement(),
        -3715992351445876736L);

    t.setDaemon(this.isDaemon);
    if (t.getPriority() != 5) {
      t.setPriority(5);
    }

    return t;
  }

  /**
   * @return the group
   */
  public ThreadGroup getGroup() {
    return group;
  }

}
