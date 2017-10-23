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

import com.epocharch.kuroro.common.inner.exceptions.NetException;
import com.epocharch.kuroro.common.inner.producer.ProducerService;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.producer.netty.DefaultInvoker;

public class ProducerServiceImpl implements ProducerService {

	public ProducerServiceImpl() {

	}

	@Override
	public Wrap sendMessage(Wrap msg) throws NetException, InterruptedException {
		Wrap ret = null;
		if (msg.isACK()) {
			ret = DefaultInvoker.getInstance().invokeSync(msg);
		} else {
			DefaultInvoker.getInstance().invokeCallback(msg, null);
		}
		return ret;
	}

}
