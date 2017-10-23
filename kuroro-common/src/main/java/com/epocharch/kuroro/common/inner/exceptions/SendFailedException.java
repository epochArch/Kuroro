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

public class SendFailedException extends Exception {

  private static final long serialVersionUID = 4638178137354823575L;

  public SendFailedException(String message) {
    super(message);
  }

  public SendFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public SendFailedException(Throwable cause) {
    super(cause);
  }
}
