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


package com.epocharch.kuroro.consumer.impl.inner;

import com.epocharch.kuroro.common.inner.wrap.WrappedConsumerMessage;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端读超时处理
 * @author bill
 * @date 12/18/14.
 */
public class HeartBeatHandler extends IdleStateHandler{

    private static final Logger logger = LoggerFactory.getLogger(HeartBeatHandler.class);

    public HeartBeatHandler(Timer timer, int readerIdleTimeSeconds, int writerIdleTimeSeconds, int allIdleTimeSeconds) {
        super(timer, readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleState state, long lastActivityTimeMillis) throws Exception {
        Channel channel = ctx.getChannel();
        switch (state) {
            case READER_IDLE:
                logger.info("Read timeout, channel {} connected to {} disconnected", channel.getLocalAddress(), channel.getRemoteAddress());
                channel.close();
                break;
            case WRITER_IDLE:
                logger.debug("Write timeout, send heartbeat to {}", channel.getRemoteAddress());
                if (channel.isOpen())
                    ctx.getChannel().write(WrappedConsumerMessage.HEARTBEAT);
                else
                    logger.warn("channel was closed ! ", channel);
                break;
        }
    }
}
