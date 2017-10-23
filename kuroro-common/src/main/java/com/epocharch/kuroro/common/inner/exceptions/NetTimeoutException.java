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

package com.epocharch.kuroro.common.inner.exceptions;

public class NetTimeoutException extends NetException {

  private static final long serialVersionUID = -4007036571413041726L;

  public NetTimeoutException() {
    super();
  }

  public NetTimeoutException(String message) {
    super(message);
  }

  public NetTimeoutException(Throwable cause) {
    super(cause);
  }

  public NetTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

}
