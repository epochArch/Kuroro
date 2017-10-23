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


package com.epocharch.kuroro.producer.impl;

import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.lmax.disruptor.EventFactory;

public final class WrappedMessageContainer {

	private Wrap wrappedMessage;

	public Wrap getWrappedMessage() {
		return wrappedMessage;
	}

	public void setWrappedMessage(Wrap wrappedMessage) {
		this.wrappedMessage = wrappedMessage;
	}

	public final static EventFactory<WrappedMessageContainer> EVENT_FACTORY = new EventFactory<WrappedMessageContainer>() {

		@Override
		public WrappedMessageContainer newInstance() {
			return new WrappedMessageContainer();
		}

	};

}
