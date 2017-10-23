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

import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 静默丢弃
 * Created by bill on 11/24/14.
 */
public class DiscardAsyncRejectionPolicy implements AsyncRejectionPolicy{
    public static final Logger logger = LoggerFactory.getLogger(DiscardAsyncRejectionPolicy.class);
    @Override
    public void onRejected(KuroroMessage kuroroMessage, Producer producer) {
        logger.error("Discard a kuroro message: {}", kuroroMessage);
    }
}
