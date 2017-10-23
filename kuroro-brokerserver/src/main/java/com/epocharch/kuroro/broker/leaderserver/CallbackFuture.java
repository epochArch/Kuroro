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

package com.epocharch.kuroro.broker.leaderserver;

import com.epocharch.kuroro.common.inner.exceptions.NetException;
import com.epocharch.kuroro.common.inner.exceptions.NetTimeoutException;
import com.epocharch.kuroro.common.netty.component.RequestError;
import org.jboss.netty.channel.ChannelFuture;

public class CallbackFuture implements Runnable {

  private ChannelFuture future;

  private boolean done = false;
  private boolean cancelled = false;
  private RequestError error;
  private LeaderClient client;
  private Long createdMillisTime;

  private MessageIDPair messageIDPair;

  @Override
  public void run() {
    synchronized (this) {
      this.done = true;
      this.notifyAll();
    }
  }

  public MessageIDPair get(long timeoutMillis) throws InterruptedException, NetException {
    synchronized (this) {
      long start = this.createdMillisTime;
      while (!this.done) {
        long timeoutMillis_ = timeoutMillis - (System.currentTimeMillis() - start);
        if (timeoutMillis_ <= 0) {
          this.error = RequestError.TIMEOUT;
          StringBuffer sb = new StringBuffer();
          sb.append(this.error.getMsg()).append("\r\n host:").append(client.getHost()).append(":")
              .append(client.getPort());
          throw new NetTimeoutException(sb.toString());
        } else {
          this.wait(timeoutMillis_);
        }
      }
      return this.messageIDPair;
    }
  }

  public boolean cancel() {
    synchronized (this) {
      this.cancelled = this.future.cancel();
      this.notifyAll();
    }

    return this.cancelled;
  }

  public boolean isCancelled() {
    return this.cancelled;
  }

  public boolean isDone() {
    return this.done;
  }

  public LeaderClient getClient() {
    return this.client;
  }

  public void setClient(LeaderClient client) {
    this.client = client;
  }

  public void setFuture(ChannelFuture future) {
    this.future = future;
  }

  public void setMessageIDPair(MessageIDPair messageIDPair) {
    this.messageIDPair = messageIDPair;
  }

  public Long getCreatedMillisTime() {
    return createdMillisTime;
  }

  public void setCreatedMillisTime(Long createdMillisTime) {
    this.createdMillisTime = createdMillisTime;
  }
}
