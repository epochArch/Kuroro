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
import org.jboss.netty.channel.Channel;

public interface SimpleClient {

  public void connect() throws NetException;

  public void write(Wrap wrap);

  public SimpleFuture write(Wrap wrap, SimpleCallback callback);

  public boolean isConnected();

  public boolean isActive();

  public void setActive(boolean active);

  public boolean isActiveSetable();

  public void setActiveSetable(boolean activesetable);

  public boolean isWritable();

  public String getHost();

  public String getAddress();

  public int getPort();

  public Channel getChannel();

  public void close();

  public void doWrap(Wrap wrap);

  public void dispose();
}
