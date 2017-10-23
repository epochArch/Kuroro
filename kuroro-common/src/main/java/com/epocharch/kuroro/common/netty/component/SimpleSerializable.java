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
import java.io.Serializable;

public interface SimpleSerializable extends Serializable {

  public byte getSerializ();

  public long getSequence();

  public void setSequence(long seq);

  public Object getObject() throws NetException;

  public Object getContext();

  public void setContext(Object context);

}
