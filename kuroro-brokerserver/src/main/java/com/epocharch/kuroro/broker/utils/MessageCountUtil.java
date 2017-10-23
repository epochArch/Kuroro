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

package com.epocharch.kuroro.broker.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhoufeiqiang on 11/10/2017.
 */
public class MessageCountUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(MessageCountUtil.class);
  ;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

  public MessageCountUtil() {
  }

  public void SubmitProducer(Runnable task) {
    scheduler.scheduleWithFixedDelay(task, 3, 3, TimeUnit.SECONDS);
  }

  public void SubmitConsumer(Runnable task) {
    scheduler.scheduleWithFixedDelay(task, 3, 3, TimeUnit.SECONDS);
  }

  public void close() {
    scheduler.shutdown();
  }
}
