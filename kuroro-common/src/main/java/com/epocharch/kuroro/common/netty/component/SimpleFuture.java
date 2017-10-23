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
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import java.util.concurrent.TimeUnit;

public interface SimpleFuture {

  public Wrap get() throws InterruptedException, NetException;

  public Wrap get(long timeoutMillis) throws InterruptedException, NetException;

  public Wrap get(long timeout, TimeUnit unit) throws InterruptedException, NetException;

  public boolean cancel();

  public boolean isCancelled();

  public boolean isDone();

  public SimpleClient getClient();

  public void setClient(SimpleClient client);
}
