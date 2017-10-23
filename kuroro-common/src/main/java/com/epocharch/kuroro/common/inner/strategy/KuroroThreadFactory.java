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

package com.epocharch.kuroro.common.inner.strategy;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuroroThreadFactory implements ThreadFactory, Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(KuroroThreadFactory.class);
  private final static String PREFIX_DEFAULT = "kuroro-";
  private static ThreadGroup topThreadGroup = new ThreadGroup("kuroro-top");
  private String prefix;

  private List<WeakReference<Thread>> threadList = Collections
      .synchronizedList(new ArrayList<WeakReference<Thread>>());
  private ConcurrentHashMap<String, AtomicInteger> prefixToSeq = new ConcurrentHashMap<String, AtomicInteger>();

  public KuroroThreadFactory(String namePrefix) {
    if (namePrefix != null) {
      this.prefix = namePrefix;
    } else {
      this.prefix = PREFIX_DEFAULT;
    }

  }

  public KuroroThreadFactory() {
    this(PREFIX_DEFAULT);
  }

  public static ThreadGroup getTopThreadGroup() {
    return topThreadGroup;
  }

  @Override
  public Thread newThread(Runnable r) {
    return newThread(r, "");
  }

  public Thread newThread(Runnable r, String threadNamePrefix) {
    prefixToSeq.putIfAbsent(threadNamePrefix, new AtomicInteger(1));
    Thread t = new Thread(topThreadGroup, r,
        prefix + threadNamePrefix + prefixToSeq.get(threadNamePrefix).getAndIncrement());
    threadList.add(new WeakReference<Thread>(t));
    return t;
  }

  @Override
  public void close() {
    for (WeakReference<Thread> ref : threadList) {
      Thread t = ref.get();
      if (t != null && t.isAlive()) {
        if (t instanceof Closeable) {
          try {
            ((Closeable) t).close();
          } catch (Exception e) {
            LOG.error("unexpected exception", e);
          }
        }
        t.interrupt();
      }
    }
  }
}
