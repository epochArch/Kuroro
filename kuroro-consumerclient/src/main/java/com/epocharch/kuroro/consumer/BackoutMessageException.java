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
 * 当consumer无法处理Message且希望重试该消息时，可以抛出该异常，MQ 接收到该异常会使用重试策略
 *
 */
public class BackoutMessageException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 48417034870355948L;

	public BackoutMessageException() {
	}

	public BackoutMessageException(String message, Throwable cause) {
		super(message, cause);
	}

	public BackoutMessageException(String message) {
		super(message);
	}

	public BackoutMessageException(Throwable cause) {
		super(cause);
	}

}
