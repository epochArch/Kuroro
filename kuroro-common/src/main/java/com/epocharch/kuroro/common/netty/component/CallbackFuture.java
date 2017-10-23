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

import com.epocharch.kuroro.common.inner.exceptions.NetException;
import com.epocharch.kuroro.common.inner.exceptions.NetTimeoutException;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.ChannelFuture;

public class CallbackFuture implements SimpleCallback, SimpleFuture {

  private ChannelFuture future;
  private Wrap ackMsg;
  private Wrap Msg;
  private boolean done = false;
  private boolean cancelled = false;
  private RequestError error;
  private SimpleClient client;

  @Override
  public void run() {
    synchronized (this) {
      this.done = true;
      this.notifyAll();
    }
  }

  @Override
  public Wrap get() throws InterruptedException, NetException {
    return get(Long.MAX_VALUE);
  }

  @Override
  public Wrap get(long timeoutMillis) throws InterruptedException, NetException {
    synchronized (this) {
      long start = Msg.getCreatedMillisTime();
      while (!this.done) {
        long timeoutMillis_ = timeoutMillis - (System.currentTimeMillis() - start);
        if (timeoutMillis_ <= 0) {
          this.error = RequestError.TIMEOUT;
          StringBuffer sb = new StringBuffer();
          sb.append(this.error.getMsg()).append("\r\n seq:").append(Msg.getSequence())
              .append("\r\n host:")
              .append(client.getHost()).append(":").append(client.getPort());
          throw new NetTimeoutException(sb.toString());
        } else {
          this.wait(timeoutMillis_);
        }
      }
      return this.ackMsg;
    }

  }

  @Override
  public Wrap get(long timeout, TimeUnit unit) throws InterruptedException, NetException {
    return get(unit.toMillis(timeout));
  }

  @Override
  public boolean cancel() {
    synchronized (this) {
      this.cancelled = this.future.cancel();
      this.notifyAll();
    }

    return this.cancelled;
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled;
  }

  @Override
  public boolean isDone() {
    return this.done;
  }

  @Override
  public SimpleClient getClient() {
    return this.client;
  }

  @Override
  public void setClient(SimpleClient client) {
    this.client = client;
  }

  @Override
  public SimpleFuture getFuture(ChannelFuture future) {
    this.future = future;
    return this;
  }

  @Override
  public void callback(Wrap ackMsg) {
    this.ackMsg = ackMsg;
  }

  @Override
  public Wrap getWrap() {
    return this.Msg;
  }

  @Override
  public void setWrap(Wrap message) {
    this.Msg = message;
  }

}
