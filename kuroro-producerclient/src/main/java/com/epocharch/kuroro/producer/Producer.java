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

package com.epocharch.kuroro.producer;


import com.epocharch.kuroro.common.ProtocolType;
import com.epocharch.kuroro.common.inner.exceptions.SendFailedException;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.producer.impl.AsyncRejectionPolicy;
import java.util.Map;

public interface Producer {

	public String sendMessage(Object content) throws SendFailedException;

	public String sendMessage(Object content, ProtocolType protocolType)
			throws SendFailedException;

	public String sendMessage(Object content, String messageType,
                              ProtocolType protocolType) throws SendFailedException;

	public String sendMessage(Object content, Map<String, String> properties,
                              ProtocolType protocolType) throws SendFailedException;

	public String sendMessage(Object content, Map<String, String> properties,
                              String messageType, ProtocolType protocolType)
			throws SendFailedException;

	public String forwardMessage(KuroroMessage message, Boolean isProducerAck)
			throws SendFailedException;

	public AsyncRejectionPolicy getAsyncRejectionPolicy();

	public void setAsyncRejectionPolicy(AsyncRejectionPolicy asyncRejectionPolicy);

	void close();
}
