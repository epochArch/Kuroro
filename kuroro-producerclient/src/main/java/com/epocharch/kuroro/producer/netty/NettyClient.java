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

package com.epocharch.kuroro.producer.netty;


import com.epocharch.kuroro.common.inner.exceptions.NetException;
import com.epocharch.kuroro.common.inner.wrap.Wrap;
import com.epocharch.kuroro.common.inner.wrap.WrappedProducerAck;
import com.epocharch.kuroro.common.inner.wrap.WrappedType;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.SimpleCallback;
import com.epocharch.kuroro.common.netty.component.SimpleClient;
import com.epocharch.kuroro.common.netty.component.SimpleFuture;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyClient implements SimpleClient {

	private static final Logger LOG = LoggerFactory
			.getLogger(NettyClient.class);
	private static final int DEFAULT_BOSS_COUNT = 1;
	private ClientBootstrap bootstrap;
	private Channel channel;
	private String host;
	private int port = 20000;
	private String address;
	private int connectTimeout = 3000;
	private volatile boolean connected = false;
	private volatile boolean closed = true;
	private volatile boolean active = true;
	private volatile boolean activeSetable = false;

	public NettyClient(String host, int port) {
		this.host = host;
		this.port = port;
		this.address = host + ":" + port;
		int cpus = Runtime.getRuntime().availableProcessors();
		if (cpus >= 8) {
			cpus = 3 + ((5 * cpus) / 8);
		}
		Timer timer = new HashedWheelTimer(new DefaultThreadFactory(
				"Producer-Client-HashedWheelTimer"));
		this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				ClientManagerFactory.getClientManager().getBossExecutor(), DEFAULT_BOSS_COUNT,
				new NioWorkerPool(ClientManagerFactory.getClientManager().getWorkerExecutor(), cpus * 2),
				timer));
		this.bootstrap.setPipelineFactory(new ProducerClientPipeLineFactory(
				this, ClientManagerFactory.getClientManager().getClientResponseThreadPool()));
	}

	@Override
	public synchronized void connect() throws NetException {
		if (this.connected || !this.closed) {
			return;
		}
		ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
				port));
		// 等待连接创建成功
		if (future.awaitUninterruptibly(this.connectTimeout,
				TimeUnit.MILLISECONDS)) {
			if (future.isSuccess()) {
				LOG.info("Client is conneted to " + this.host + ":" + this.port);
				this.connected = true;
				this.closed = false;
			} else {
				LOG.warn("Client is not conneted to " + this.host + ":"
						+ this.port);
			}
		}
		this.channel = future.getChannel();
	}

	@Override
	public void write(Wrap wrap) {
		write(wrap, null);
	}

	@Override
	public SimpleFuture write(Wrap wrap, SimpleCallback callback) {
		Object msg = wrap;
		ChannelFuture future = null;

		if (channel == null ) {
			LOG.error("channel:" + null + " ^^^^^^^^^^^^^^");
		} else {

			future = channel.write(msg);
			if (wrap.getWrappedType() == WrappedType.OBJECT_MSG) {
				// future listener暂时不加
				future.addListener(new MsgWriteListener(wrap));
			}
		}
		if (callback != null) {
			callback.setClient(this);
			callback.setWrap(wrap);
			return callback.getFuture(future);

		} else {
			return null;
		}
	}

	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public boolean isActive() {
		return active;
	}

	@Override
	public void setActive(boolean active) {
		if (this.activeSetable) {
			this.active = active;
		}
	}

	@Override
	public boolean isActiveSetable() {
		return activeSetable;
	}

	@Override
	public void setActiveSetable(boolean activeSetable) {
		this.activeSetable = activeSetable;
	}

	@Override
	public boolean isWritable() {
		return this.channel.isWritable();
	}

	@Override
	public String getHost() {
		return host;
	}

	@Override
	public String getAddress() {
		return this.address;
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public Channel getChannel() {
		return channel;
	}

	public boolean equals(Object obj) {
		if (obj instanceof NettyClient) {
			NettyClient nc = (NettyClient) obj;
			return this.address.equals(nc.getAddress());
		} else {
			return super.equals(obj);
		}
	}

	@Override
	public int hashCode() {
		return address.hashCode();
	}

	@Override
	public void close() {
		closed = true;
		connected = false;
        if (channel != null) {
            channel.close();
        }
    }

	@Override
	public void doWrap(Wrap ackMsg) {
		BrokerGroupRouteManagerFactory.getInstacne().processAckMsg(ackMsg);
	}

	public class MsgWriteListener implements ChannelFutureListener {

		private Wrap msg;

		public MsgWriteListener(Wrap msg) {
			this.msg = msg;
		}

		@Override
		public void operationComplete(ChannelFuture channelfuture)
				throws Exception {
			if (channelfuture.isSuccess()) {
				return;
			}else{
				LOG.error("channel was closed!  please retry produce message!", channelfuture.getCause());
			}
			Wrap wrapRet = new WrappedProducerAck(
					"Write to broker channel error.", 203);
			wrapRet.setSequence(msg.getSequence());
			doWrap(wrapRet);

		}

	}

	@Override
	public void dispose() {
		// 公用的bossExecutors线程池，会把所有的client stop；
		// if(this.bootstrap!=null)
		// this.bootstrap.releaseExternalResources();
	}

}
