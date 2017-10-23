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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerConfig {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ProducerConfig.class);
	public static final int MAX_THREADPOOL_SIZE = 100;
	public static final SendMode DEFAULT_PRODUCER_MODE = SendMode.SYNC_MODE;
	public static final int DEFAULT_ASYNC_RETRY_TIMES = 5;
	public static final int DEFAULT_SYNC_RETRY_TIMES = 5;
	public static final boolean DEFAULT_ZIPPED = false;
	public static final int DEFAULT_THREADPOOL_SIZE = 1;
	public static final boolean DEFAULT_RESUME_LAST_SESSION = false;
	public static final int DEFAULT_ASYNC_QUEUE_SIZE = 1024;
	public static final int DEFAULT_HESSIAN_COMPRESSIONTHRESHOLD = 1024; // 基数是k
	public static final int DEFAULT_ASYNC_PUT_SPIN_COUNT = 3;

	private SendMode mode = DEFAULT_PRODUCER_MODE;
	private int asyncRetryTimes = DEFAULT_ASYNC_RETRY_TIMES;
	private int syncRetryTimes = DEFAULT_SYNC_RETRY_TIMES;
	private boolean zipped = DEFAULT_ZIPPED;
	private int threadPoolSize = DEFAULT_THREADPOOL_SIZE;
	private boolean resumeLastSession = DEFAULT_RESUME_LAST_SESSION;
	private int asyncQueueSize = DEFAULT_ASYNC_QUEUE_SIZE;
	private int hessianCompressionThreshold = DEFAULT_HESSIAN_COMPRESSIONTHRESHOLD;
	private int asyncPutSpinCount = DEFAULT_ASYNC_PUT_SPIN_COUNT;

	private volatile boolean sendFlag =  true;

	private boolean idcProdct = false;

	/**
	 * @return Producer工作模式，类型为{@link SendMode}
	 */
	public SendMode getMode() {
		return mode;
	}

	/**
	 * 设置消息发送模式，默认为{@link SendMode}<code>.SYNC_MODE</code>
	 *
	 * @param mode
	 *            Producer工作模式
	 */
	public void setMode(SendMode mode) {
		this.mode = mode;
	}

	/**
	 * @return 发送失败重试次数
	 */
	public int getAsyncRetryTimes() {
		return asyncRetryTimes;
	}

	/**
	 * 设置异步发送重试次数，默认为10
	 * 
	 * @param asyncRetryTimes
	 */
	public void setAsyncRetryTimes(int asyncRetryTimes) {
		if (asyncRetryTimes < 0) {
			this.asyncRetryTimes = DEFAULT_ASYNC_RETRY_TIMES;
			LOGGER.warn("invalid asyncRetryTimes, use default value: "
					+ this.asyncRetryTimes + ".");
			return;
		}
		this.asyncRetryTimes = asyncRetryTimes;
	}

	/**
	 * @return 是否对待发送消息进行压缩
	 */
	public boolean isZipped() {
		return zipped;
	}

	/**
	 * 设置是否压缩存储消息，默认为false<br>
	 * 尝试进行压缩，如果压缩失败，则原文存储
	 * 
	 * @param zipped
	 *            是否压缩
	 */
	public void setZipped(boolean zipped) {
		this.zipped = zipped;
	}

	/**
	 * @return 异步模式下线程池大小
	 */
	public int getThreadPoolSize() {
		return threadPoolSize;
	}

	/**
	 * 设置异步模式（{@link SendMode}<code>.SYNC_MODE</code>
	 * ）从MemoryQueue获取并发送消息的线程数量，默认为1
	 * 
	 * @param threadPoolSize
	 *            线程池大小
	 */
	public void setThreadPoolSize(int threadPoolSize) {
		if (threadPoolSize <= 0 || threadPoolSize > MAX_THREADPOOL_SIZE) {
			this.threadPoolSize = DEFAULT_THREADPOOL_SIZE;
			LOGGER.warn("invalid threadPoolSize, must between 1 - "
					+ MAX_THREADPOOL_SIZE + ", use default value: "
					+ this.threadPoolSize + ".");
			return;
		}
		this.threadPoolSize = threadPoolSize;
	}

	/**
	 * 
	 * @return异步模式时，重启Producer是否继续上次未完成的发送
	 */
	public boolean isResumeLastSession() {
		return resumeLastSession;
	}

	/**
	 * 设置重启producer时是否发送上次未发送的消息，默认为false
	 * 
	 * @param resumeLastSession
	 */
	public void setResumeLastSession(boolean resumeLastSession) {
		this.resumeLastSession = resumeLastSession;
	}
	
	
	public boolean isSendFlag() {
		return sendFlag;
	}
	
	/*
	 * control producerclient send message
	 * 
	 * */
	public void setSendFlag(boolean sendFlag) {
		this.sendFlag = sendFlag;
	}

	public boolean isIdcProdct() {
		return  idcProdct;
	}

	public void setIdcProdct(boolean idcProdction) {
		this.idcProdct = idcProdct;
	}

	@Override
	public String toString() {
		return "Mode="
				+ getMode()
				+ "; Zipped="
				+ isZipped()
				+ (getMode() == SendMode.ASYNC_MODE ? "; ThreadPoolSize="
						+ getThreadPoolSize() + "; SendMsgLeftLastSession="
						+ isResumeLastSession() + "; AsyncRetryTimes="
						+ getAsyncRetryTimes() : "; SyncRetryTimes="
						+ getSyncRetryTimes()) + "; AsyncQueueSize="
				+ getAsyncQueueSize() + "; hessianCompressionThreshold="
				+ getHessianCompressionThreshold()+"; sendFlag="
				+isSendFlag() + "; idcProduct="+isIdcProdct();
	}

	public int getSyncRetryTimes() {
		return syncRetryTimes;
	}

	/**
	 * 设置同步模式发送失败重试次数，默认为0
	 * 
	 * @param syncRetryTimes
	 *            发送失败重试次数
	 */
	public void setSyncRetryTimes(int syncRetryTimes) {
		if (syncRetryTimes < 0) {
			this.syncRetryTimes = DEFAULT_SYNC_RETRY_TIMES;
			LOGGER.warn("invalid syncRetryTimes, use default value: "
					+ this.syncRetryTimes + ".");
			return;
		}
		this.syncRetryTimes = syncRetryTimes;
	}

	public int getAsyncQueueSize() {
		return this.asyncQueueSize;
	}

	/*
	 * 异步情况下，排队消息的个数
	 */
	public void setAsyncQueueSize(int asyncQueueSize) {
		if (!is2Power(asyncQueueSize)) {
			this.asyncQueueSize = DEFAULT_ASYNC_QUEUE_SIZE;
			LOGGER.warn("invalid asyncQueueSize must be 2's power, use default value: "
					+ this.asyncQueueSize + ".");
			return;
		}
		this.asyncQueueSize = asyncQueueSize;
	}

	// 检验是否2的power
	private boolean is2Power(int number) {
		if (number <= 0)
			return false;
		return (number | (number - 1)) == (2 * number - 1);
	}

	public int getHessianCompressionThreshold() {
		return hessianCompressionThreshold;
	}

	/*
	 * hessianCompressionThreshold默认单位是k
	 */
	public void setHessianCompressionThreshold(int hessianCompressionThreshold) {
		if (hessianCompressionThreshold < 0) {
			this.hessianCompressionThreshold = DEFAULT_HESSIAN_COMPRESSIONTHRESHOLD;
			LOGGER.warn("invalid hessianCompressionThreshold, use default value: "
					+ this.hessianCompressionThreshold + ".");
			return;
		}
		this.hessianCompressionThreshold = hessianCompressionThreshold;
	}

	public int getAsyncPutSpinCount() {
		return asyncPutSpinCount;
	}

	/**
	 * 异步发送时，send方法往异步队列中放入消息时的spin次数
	 * @param asyncPutSpinCount
	 */
	public void setAsyncPutSpinCount(int asyncPutSpinCount) {
		if (asyncPutSpinCount < 0) {
			LOGGER.error("Invalid asyncPutSpinCount {}", asyncPutSpinCount);
			throw new IllegalArgumentException("Invalid asyncPutSpinCount");
		}
		this.asyncPutSpinCount = asyncPutSpinCount;
	}

}