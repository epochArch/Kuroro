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


import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.inner.util.IPUtil;
import com.epocharch.kuroro.common.inner.util.NameCheckUtil;
import com.epocharch.kuroro.common.inner.wrap.WrappedConsumerMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.message.Destination;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.protocol.json.JsonDecoder;
import com.epocharch.kuroro.common.protocol.json.JsonEncoder;
import com.epocharch.kuroro.consumer.BrokerGroupRouteManager;
import com.epocharch.kuroro.consumer.Consumer;
import com.epocharch.kuroro.consumer.ConsumerConfig;
import com.epocharch.kuroro.consumer.MessageListener;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerImpl implements Consumer {
    private static final Logger LOG = LoggerFactory
            .getLogger(ConsumerImpl.class);
    private volatile ConsumerThread consumerThread;
    private InetSocketAddress consumerAddress;
    private String consumerId;
    private Destination dest;
    private ClientBootstrap bootstrap;
    private MessageListener listener;
    private volatile boolean closed = false;
    private volatile AtomicBoolean started = new AtomicBoolean(false);
    private ConsumerConfig config;
    private final String consumerIP = IPUtil.getFirstNoLoopbackIP4Address();
    private  BrokerGroupRouteManager routerManager;
    private final Map<String, LinkedBlockingQueue<Consumer>> connectionPool;
    private final long connectInterval = 5000L;

    private static final int READ_TIMEOUT_SECONDS = 60;
    private static final int WRITE_TIMEOUT_SECONDS = 20;
 
    private final List<ConsumerImpl> children = new ArrayList<ConsumerImpl>();
    private AtomicBoolean isRestart = new AtomicBoolean(false);
    private MessageClientHandler handler;
    private static Timer timer = new HashedWheelTimer(new DefaultThreadFactory(
            "Consumer-Client-HashedWheelTimer"));
    private Thread nonDameoThread;
    private KuroroThreadFactory kuroroThreadFactory = new KuroroThreadFactory("consumerThread-nonDameoThread");

    public ConsumerImpl(Destination dest, String consumerId,
                        ConsumerConfig config, InetSocketAddress consumerAddress,
                        BrokerGroupRouteManager routerManager,
                        Map<String, LinkedBlockingQueue<Consumer>> connectionPool) {
        if (!NameCheckUtil.isConsumerIdValid(consumerId)) {
            throw new IllegalArgumentException("ConsumerId is invalid : consumerId由字母,数字,减号“-”和下划线“_”构成，只能以字母开头，长度为2到30 ! ");
        }
        this.dest = dest;
        this.consumerId = consumerId;
        this.config = config == null ? new ConsumerConfig() : config;
        this.connectionPool = connectionPool;
        this.routerManager = routerManager;
        this.consumerAddress =consumerAddress;

    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void start() {
        LOG.info("Starting " + this.toString());
        if (listener == null) {
            throw new IllegalArgumentException(
                    "MessageListener is null, MessageListener should be set(use setListener()) before start.");
        }
        if (consumerAddress == null)
            return;
        if (started.compareAndSet(false, true)) {
            init();
            startConsumerThread();
            isRestart.set(false);
        }
        LOG.info("Consumer started: {}",this);
        for (Consumer child : children) {
            try {
                child.start();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Child consumer started error: {}", child);
            }
        }

        this.nonDameoThread = kuroroThreadFactory.newThread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        if (consumerThread.getIsClosed().get()) {
                          startConsumerThread();
                            LOG.warn("start new ConsumerThread === topic : " + dest.getName()
                                    + " , consumerId : " + consumerId );
                        }
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }

        });
        nonDameoThread.start();
    }

    @Override
    public void setListener(MessageListener messagelistener) {
        this.listener = messagelistener;
        for (ConsumerImpl child : children) {
            child.setListener(messagelistener);
        }
    }

    private void init() {
        LOG.info("Initializing consumer named {} of topic {}", consumerId, dest.getName());

        bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(new DefaultThreadFactory(
                        "Consumer-Client-BossExecutor-" + dest.getName() + "-" + consumerId)),
                Executors.newCachedThreadPool(new DefaultThreadFactory(
                        "Consumer-Client-WorkerExecutor-" + dest.getName() + "-" + consumerId))));

        final ConsumerImpl consumer = this;
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                handler = new MessageClientHandler(
                        consumer);
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("heartbeat", new HeartBeatHandler(timer, READ_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS, 0));
                pipeline.addLast("frameDecoder",
                        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0,
                                4, 0, 4));
                pipeline.addLast("jsonDecoder", new JsonDecoder(
                        WrappedMessage.class));
                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                pipeline.addLast("jsonEncoder", new JsonEncoder(
                        WrappedConsumerMessage.class));
                pipeline.addLast("handler", handler);

                return pipeline;
            }
        });
        closed = false;
        try {
            String key = dest.getName() + consumerAddress.getAddress() + ":"
                    + consumerAddress.getPort();
            if (connectionPool.containsKey(key)) {
                LinkedBlockingQueue<Consumer> consumerArr = connectionPool
                        .get(key);
                consumerArr.put(this);
            } else {
                LinkedBlockingQueue<Consumer> consumerArr = new LinkedBlockingQueue<Consumer>();
                consumerArr.put(this);
                connectionPool.put(key, consumerArr);
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void close(){
           String key = dest.getName() + consumerAddress.getAddress() + ":"
                   + consumerAddress.getPort();
           LinkedBlockingQueue<Consumer> _consumersQueue = null;
           if(connectionPool.containsKey(key)){
           	 _consumersQueue = connectionPool.get(key);
           	for(Consumer consumer : _consumersQueue ){
           		ConsumerImpl consumerImpl = (ConsumerImpl) consumer;
   	              if (consumerImpl.consumerThread != null) {
   	            	  consumerImpl.consumerThread.interrupt();
   	             }
   	          try {
   	              if(consumerImpl.handler != null) {
   	                  ChannelGroup group = consumerImpl.handler.getChannelGroup();
   	                  if(group.disconnect().isCompleteFailure()){
   	                      LOG.info("group disconnect is failure ....");
   	                  }else {
   	                      group.unbind().await();
   	                  }
   	                  group.close().await();
   	                  group.clear();
   	              }
   	              consumerImpl.bootstrap.releaseExternalResources();
   	          } catch (InterruptedException e) {
   	              LOG.error(e.getMessage(), e.getCause());
   	          }
   	          consumerImpl.started.set(false);
   	  
   	          for (ConsumerImpl child : children) {
   	              try {
   	                  child.close();
   	              } catch (Exception e) {
   	                  e.printStackTrace();
   	                  LOG.error("Close consumer error: {}", child);
   	              }
   	          }
           	}
           }
           if(_consumersQueue != null){
           		_consumersQueue.clear();
           		_consumersQueue = null;
           }
           connectionPool.remove(key);
           closed = true;
    }

    @Override
    public void restart() {
        if (!isRestart.compareAndSet(false, true))
            return;
        close();
        try {
            while (true) {
                Thread.sleep(1500L);
                consumerAddress = getConsumerAddress();
                if (consumerAddress != null) {
                    start();
                    LOG.warn("Switch to host:" + consumerAddress.getAddress()
                            + ":" + consumerAddress.getPort());
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

        for (ConsumerImpl child : children) {
            try {
                child.restart();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Restart consumer error: {}", child);
            }
        }
    }

    public List<ConsumerImpl> getChildren() {
        return children;
    }

    public synchronized void addChild(ConsumerImpl consumer) {
        this.children.add(consumer);
    }

    public synchronized void removeChild(String topic) {
        Iterator<ConsumerImpl> it = this.children.iterator();
        while (it.hasNext()) {
            ConsumerImpl child = it.next();
            if (child.dest.getName().equals(topic)) {
                it.remove();
            }
        }
    }

    public boolean isConsumerExist(Destination dest) {
        for (ConsumerImpl impl : this.children) {
            if (impl.getDest().getName().equals(dest.getName())) {
                return true;
            }
        }
        return false;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public Destination getDest() {
        return dest;
    }

    public ClientBootstrap getBootstrap() {
        return bootstrap;
    }

    public MessageListener getListener() {
        return listener;
    }

    public ConsumerConfig getConfig() {
        return config;
    }

    public String getConsumerIP() {
        return consumerIP;
    }

    @Override
    public String getRemoteAddress() {
        return consumerAddress.getAddress().getHostAddress() + ":"
                + consumerAddress.getPort();
    }

    public InetSocketAddress getConsumerAddress() {
        HostInfo hi = routerManager.route();
        if (hi == null)
            return null;
        return new InetSocketAddress(hi.getHost(), hi.getPort());
    }

    @Override
    public String toString() {
        return String
                .format("ConsumerImpl [consumerId=%s, topicName=%s, consumerAddress=%s,  config=%s]",
                        consumerId, dest.getName(), consumerAddress, config);
    }
    
    @Override
	public void setBrokerGroupRotueManager(BrokerGroupRouteManager routeManager) {
		this.routerManager=routeManager;
	}

	private void startConsumerThread(){
	    if (consumerThread != null) {
	        consumerThread = null;
        }
        consumerThread = new ConsumerThread();
        consumerThread.setBootstrap(bootstrap);
        consumerThread.setRemoteAddress(consumerAddress);
        consumerThread.setInterval(connectInterval);
        consumerThread.setName("consumerThread");
        consumerThread.setConsumer(this);
        consumerThread.start();
    }

}
