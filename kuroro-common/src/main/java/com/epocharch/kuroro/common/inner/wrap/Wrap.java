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

package com.epocharch.kuroro.common.inner.wrap;

import java.io.Serializable;

public abstract class Wrap implements Serializable {

  private static final long serialVersionUID = 7081505877510896334L;
  private WrappedType wrappedType;
  private boolean isACK;
  private long sequence;
  private long createdMillisTime;

  public WrappedType getWrappedType() {
    return wrappedType;
  }

  public void setWrappedType(WrappedType wrappedType) {
    this.wrappedType = wrappedType;
  }

  public boolean isACK() {
    return isACK;
  }

  public void setACK(boolean isACK) {
    this.isACK = isACK;
  }

  public long getSequence() {
    return sequence;
  }

  public void setSequence(long sequence) {
    this.sequence = sequence;
  }

  public long getCreatedMillisTime() {
    return createdMillisTime;
  }

  public void setCreatedMillisTime(long createdMillisTime) {
    this.createdMillisTime = createdMillisTime;
  }

}
