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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 失败时累加failCnt，sleep(min(delayBase * failCnt, delayUpperbound))，成功时清零failCnt
 */
public class DefaultPullStrategy implements PullStrategy {

  private static Logger log = LoggerFactory.getLogger(DefaultPullStrategy.class);
  private final int delayBase;
  private final int delayUpperbound;
  private int failCnt = 0;

  public DefaultPullStrategy(int delayBase, int delayUpperbound) {
    this.delayBase = delayBase;
    this.delayUpperbound = delayUpperbound;
  }

  @Override
  public long fail(boolean shouldSleep) throws InterruptedException {
    failCnt++;
    long sleepTime = (long) failCnt * delayBase;
    sleepTime = sleepTime > delayUpperbound ? delayUpperbound : sleepTime;
    if (shouldSleep) {
      if (log.isDebugEnabled()) {
        log.debug("sleep " + sleepTime + " at " + this.getClass().getSimpleName());
      }
      Thread.sleep(sleepTime);
    }
    return sleepTime;
  }

  @Override
  public void succeess() {
    failCnt = 0;
  }

}