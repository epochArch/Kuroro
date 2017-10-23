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


package com.epocharch.kuroro.consumer;

/**
 * 当consumer希望不记录ack，以待后续重发的情况下抛出此异常 <br/>
 * <b>重要:</b>前提是使用CLIENT_ACKNOWLEDGE，且打开补偿机制
 *
 */
public class NeedResendException extends Exception {
	/**
	 *
	 */
	private static final long serialVersionUID = 4842234870355948L;

	public NeedResendException() {
	}

	public NeedResendException(String message, Throwable cause) {
		super(message, cause);
	}

	public NeedResendException(String message) {
		super(message);
	}

	public NeedResendException(Throwable cause) {
		super(cause);
	}

}
