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

package com.epocharch.kuroro.common.memeory;

import com.epocharch.kuroro.common.inner.exceptions.MemeoryQueueException;
import com.epocharch.kuroro.common.inner.strategy.DefaultPullStrategy;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现内存映射对象的临时Queue，下一个版本后续提供实现文件临时缓存。
 *
 * @author
 */
public class MemeoryQueueImpl<T> implements MemeoryQueue<T> {

  private static final Logger LOG = LoggerFactory.getLogger(MemeoryQueueImpl.class);
  private final MemeoryQueueConfig config;
  private final String queueName;
  private final LinkedBlockingQueue<MemCachedMessage> memcacheQueue;
  private final MemeoryQueueMeta meta;
  private final MemeoryQueueDataFile dataFile;
  private final CountDownLatch latch;
  private final ReentrantLock addLock;
  private final ReentrantLock getLock;
  private final ConcurrentMap<String, AtomicLong> fileMap;
  private ObjectOutputStream dataFileOutputStream;
  private AtomicLong memCacheLoadPos;
  private String memCacheFileReading;
  private Thread readerTask;
  private AtomicBoolean stopped;
  private AtomicReference<MemCachedMessage> lastMsg;
  private int objectOutputstreamTimes;

  public MemeoryQueueImpl(MemeoryQueueConfig config, String topic) {
    memCacheLoadPos = new AtomicLong(0L);
    latch = new CountDownLatch(1);
    stopped = new AtomicBoolean(false);
    addLock = new ReentrantLock();
    getLock = new ReentrantLock();
    fileMap = new ConcurrentHashMap<String, AtomicLong>();
    objectOutputstreamTimes = 0;
    this.config = config;
    this.queueName = topic;
    meta = new MemeoryQueueMeta(queueName, config);
    dataFile = new MemeoryQueueDataFile(queueName, config);
    memcacheQueue = new LinkedBlockingQueue<MemCachedMessage>(config.getMemeoryMaxSize());
    loadMeta();
    lastMsg = new AtomicReference<MemCachedMessage>(null);
    dataFileOutputStream = null;
    start();
    LOG.info((new StringBuilder("Inited FileQueue: ")).append(queueName).append(" readingFile: ")
        .append(memCacheFileReading)
        .append(" pos: ").append(memCacheLoadPos).toString());

  }

  private void loadMeta() {
    memCacheLoadPos.set(meta.getReadPos());
    memCacheFileReading = meta.getFileReading();
    if ("null".equalsIgnoreCase(memCacheFileReading) || KuroroUtil
        .isBlankString(memCacheFileReading)) {
      memCacheFileReading = null;
    }
    String readFileFromDiskScan = dataFile.getQueueFileName();
    if (readFileFromDiskScan == null) {
      if (memCacheFileReading != null) {
        LOG.error((new StringBuilder("meta file's fileReading: ")).append(memCacheFileReading)
            .append(" not equal to data file remaining: ").append(readFileFromDiskScan).toString());
      }
      memCacheFileReading = null;
      memCacheLoadPos.set(0L);
      return;
    }
    if (memCacheFileReading == null) {
      memCacheFileReading = readFileFromDiskScan;
      memCacheLoadPos.set(0L);
    } else if (!memCacheFileReading.equals(readFileFromDiskScan)) {
      LOG.error((new StringBuilder("meta file fileReading: ")).append(memCacheFileReading)
          .append(" not equal to ")
          .append(readFileFromDiskScan).toString());
      File f = new File(memCacheFileReading);
      if (f.exists()) {
        dataFile.adjust(memCacheFileReading);
        return;
      }
      memCacheFileReading = readFileFromDiskScan;
      memCacheLoadPos.set(0L);
    }
  }

  private void start() {
    readerTask = new Thread(new Runnable() {

      @Override
      public void run() {
        loadMessages();
      }
    }, (new StringBuilder("filequeue_cache_reader_thread_")).append(queueName).toString());
    readerTask.start();
  }

  private void switchReadFile(ObjectInputStream oi) {
    if (!dataFile.isEmpty()) {
      if (oi != null) {
        try {
          oi.close();
        } catch (IOException e) {
          LOG.error("close file failed when switching file", e);
        }
      }
      memCacheFileReading = dataFile.getQueueFileName();
      memCacheLoadPos.set(0L);
    }
  }

  private void loadMessages() {
    // if (StringUtils.isBlank(memCacheFileReading)) {
    // try {
    // latch.await();
    // } catch (InterruptedException e) {
    // LOG.error("lacth interruptted", e);
    // return;
    // }
    // switchReadFile(null);
    // }
    //
    // ObjectInputStream oi = null;
    // for (; !stopped.get(); switchReadFile(oi)) {
    // // 这里需要捕获运行时异常，防止线程跑飞，整个读线程停止。导致生产线程阻塞。
    // // try {
    // oi = openInputStream();
    // seek(oi, memCacheLoadPos.get());
    // if (Thread.interrupted() || stopped.get()) {
    // LOG.warn("read file thread:" + Thread.currentThread().getName() +
    // " is interrupted or stopped!");
    // return;
    // }
    //
    // loadFromFile(oi);
    // // } catch (Throwable e) {
    // // LOG.error("read file " + memCacheFileReading + " error:", e);
    // // }
    // if (Thread.interrupted() || stopped.get()) {
    // LOG.warn("read file thread:" + Thread.currentThread().getName() +
    // " is interrupted or stopped!");
    // return;
    // }
    //
    // }

    if (KuroroUtil.isBlankString(memCacheFileReading)) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        LOG.error("lacth interruptted", e);
        return;
      }
      switchReadFile(null);
    }

    ObjectInputStream oi = null;
    while (!stopped.get()) {
      try {
        /**
         * 执行到seek要么是第一次开始读，要么是读新的文件，要么是当前读完，单写的慢了，还是读当前文件。
         * 第一种：需要打开文件，执行seek, 第二种：需要打开文件，memCacheLoadPos的值是0 ，实际没有seek。
         * 第三种：memCacheLoadPos是读了多少，大于0，不需要重新打开文件，把前面的重新读一遍，在生产不连续时，
         * 这种情况很常见。
         */
        // oi = openInputStream();
        // seek(oi, memCacheLoadPos.get());
        // 所以可以改为如下：
        /**
         * 1 第一次开始读，memCacheLoadPos有可能>=0,但oi是null 2
         * 新的文件，memCacheLoadPos ==0，
         */
        if (memCacheLoadPos.get() == 0 || oi == null) {
          oi = openInputStream();
          seek(oi, memCacheLoadPos.get());
        }
        loadFromFile(oi);
        switchReadFile(oi);
      } catch (Throwable e) {
        LOG.error("read file " + memCacheFileReading + " error:", e);
        e.printStackTrace();
      }
    }
    LOG.warn("Read MsgFile thread[" + Thread.currentThread().getName() + "] exit");
  }

  private boolean seek(ObjectInputStream oi, long pos) {
    for (long i = 0L; i < pos; i++) {
      try {
        oi.readObject();
      } catch (Exception e) {
        LOG.error("Seek objectInputStream failed.", e);
        return false;
      }
    }
    return true;
  }

  private void loadFromFile(ObjectInputStream oi) {
    // load from oi

    DefaultPullStrategy pullStrategy = new DefaultPullStrategy(1000, 3000);
    int retryReadCount = 0;
    boolean firstMessageInFile = (memCacheLoadPos.get() == 0L);
    while (true) {
      if (Thread.interrupted() || stopped.get()) {
        Thread.currentThread().interrupt();
        return;
      }
      if (dataFile.isEmpty()) {
        AtomicLong writePos = (AtomicLong) fileMap.get(memCacheFileReading);
        if (writePos != null && memCacheLoadPos.get() >= writePos.get() - 1L) {
          delay(200);
        }
      }
      Object obj = null;
      try {
        obj = oi.readObject();
      } catch (Exception e) {

        /**
         * 通过结束符来标记文件结束，但有种理论上的风险：如果读到中间某条记录抛异常， 1 有可能反序列失败，因为程序已经更新， 2
         * 没有了读的权限，因为管理员的无聊操作。 3 文件损坏。 如上三种异常都是理论上的，一旦发生，要么退出，要么重试，这两种选择
         * 退出： 结束读该文件，该文件剩余的数据丢失。 重试：
         * 重试多久，不能无休止的重试，否则就不生产了，而且会积累大量的数据文件，重试多久。
         *
         * 不写文件结束符，只有在待读的文件多，刚开始读到中间的时候报错，如果直接退出，该文件剩余的数据就丢失了，没有读，
         * 这里为了尽量不丢失，加一个重试，在一定次数内，还是读不到，则退出，这在加文件结束符是一样面临的问题。但是比加文件结束符
         * 好的是在读快，业务线程停止生产了后，读线程势必读完，重试N次也是到退出当前循环，因为没有新的文件，还是会继续读当前文件。
         * 如果采用退出，因为没有新的文件，不能阻塞，一阻塞当前文件后面数据就丢了。
         */
        LOG.warn("retry " + retryReadCount + " read file[" + memCacheFileReading + "] failed:" + e
            .getMessage());
        // e.printStackTrace();
        retryReadCount++;
        try {
          pullStrategy.fail(true);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        if (retryReadCount >= 10) {
          LOG.warn("retry " + retryReadCount + " read file[" + memCacheFileReading
              + "] failed,reached the max times 10:" + e
              .getMessage() + "");
          break;
        }
      }
      if (obj != null) {
        memCacheLoadPos.incrementAndGet();
        addToMemCache(obj, firstMessageInFile, memCacheLoadPos.get());
        firstMessageInFile = false;
        Random random = new Random(System.currentTimeMillis());
        delayLoad(random.nextInt(2));
      } else {
        delay(20);
      }

    }

  }

  private void addToMemCache(Object data, boolean isFirstMessageInFile, long pos) {
    try {
      memcacheQueue.put(new MemCachedMessage(data, memCacheFileReading, isFirstMessageInFile, pos));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Exception occured.", e);
    }
  }

  private void delayLoad(int readLen) {
    if (readLen == 0 && dataFile.isEmpty()) {
      delay(2);
    }
  }

  private ObjectInputStream openInputStream() {
    ObjectInputStream oi = null;
    Throwable t = null;
    // 重试
    for (int i = 0; i < 3; i++) {
      try {
        oi = new ObjectInputStream(new FileInputStream(new File(memCacheFileReading)));
        t = null;
        break;
      } catch (Exception e) {
        t = e;
        delay((i + 1) * 10);

      }
    }

    if (t != null) {
      String errMsg = (new StringBuilder("Open InputStream for background thread failed. file: "))
          .append(memCacheFileReading)
          .toString();
      LOG.error(errMsg, t);
      throw new RuntimeException(errMsg, t);
    } else {
      return oi;
    }
  }

  private void delay(int i) {
    try {
      Thread.sleep(5 * (i + 1));
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  private void commitMessage() {
    MemCachedMessage msg = null;
    if ((msg = lastMsg.getAndSet(null)) != null) {
      meta.updateRead(msg.getPos(), msg.getFile());
      meta.incConsumeCount();
      if (msg.isFirstMessage()) {
        dataFile.archiveAndRemoveOldFile(msg.getFile());
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get() {
    MemCachedMessage msg = null;
    getLock.lock();
    try {
      msg = memcacheQueue.take();
      lastMsg.set(msg);
      commitMessage();
    } catch (InterruptedException e) {
      LOG.warn("interrupted when taking messsage from FileQueue");
      Thread.currentThread().interrupt();
    } finally {
      getLock.unlock();
    }

    return msg != null ? (T) msg.getData() : null;

  }

  @SuppressWarnings("unchecked")
  @Override
  public T get(long timeout, TimeUnit timeunit) {
    MemCachedMessage msg = null;
    getLock.lock();
    try {
      msg = memcacheQueue.poll(timeout, timeunit);
      lastMsg.set(msg);
      commitMessage();
    } catch (InterruptedException e) {
      LOG.warn("interrupted when taking messsage from FileQueue");
      Thread.currentThread().interrupt();
    } finally {
      getLock.unlock();
    }

    return msg != null ? (T) msg.getData() : null;
  }

  @Override
  public void add(T m) throws MemeoryQueueException {
    if (stopped.get()) {
      throw new MemeoryQueueException(
          (new StringBuilder("FileQueue has been closed. Queue name: ")).append(queueName)
              .toString());
    }
    addLock.lock();
    try {
      checkDataFileOutputStream();
      write(m);
      meta.incAddCount();
    } finally {
      addLock.unlock();
    }
    latch.countDown();
    return;
  }

  public void close() {
    stopped.set(true);
    if (readerTask != null) {
      readerTask.interrupt();
    }
    meta.close();
  }

  private void checkDataFileOutputStream() {
    FileInputStream input = null;
    try {

      if (dataFileOutputStream == null) {
        // String newDataFileName = dataFile.createQueueFile();
        // dataFileOutputStream = new ObjectOutputStream(new
        // FileOutputStream(newDataFileName));
        // meta.updateWrite(newDataFileName);
        // fileMap.put(newDataFileName, new AtomicLong(0L));
        // objectOutputstreamTimes = 0;
        this.createNewWriteFile();
      }
      input = new FileInputStream(meta.getFileWriting());
      long maxDataFileSize = config.getMaxDataFileSize();
      if ((long) input.available() >= maxDataFileSize) {
        LOG.info(
            "current writeFile:" + meta.getFileWriting() + " size is > max size:" + maxDataFileSize
                + ",will write to new file");
        dataFileOutputStream.close();
        // String newDataFileName = dataFile.createQueueFile();
        // dataFileOutputStream = new ObjectOutputStream(new
        // FileOutputStream(newDataFileName));
        // fileMap.remove(meta.getFileWriting());
        // meta.updateWrite(newDataFileName);
        // fileMap.put(newDataFileName, new AtomicLong(0L));
        // objectOutputstreamTimes = 0;
        this.createNewWriteFile();
      }
    } catch (FileNotFoundException e) {
      LOG.error("Create data file failed.", e);
      throw new RuntimeException("create data file failed.", e);
    } catch (IOException e) {
      LOG.error("Create data file failed.", e);
      throw new RuntimeException("create data file failed.", e);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          LOG.error("Close temp inputstream for size cal failed.", e);
        }
      }

    }
  }

  /**
   * 创建新的写入文件代码独立
   */
  private void createNewWriteFile() {
    try {
      String newDataFileName = dataFile.createQueueFile();
      dataFileOutputStream = new ObjectOutputStream(new FileOutputStream(newDataFileName));
      fileMap.remove(meta.getFileWriting());
      meta.updateWrite(newDataFileName);
      fileMap.put(newDataFileName, new AtomicLong(0L));
      // newDataFileName add to dataFile at here
      // 创建了新的文件，通知读文件的线程,确保文件已经创建成，而且fileMap里面是新的文件的数据，
      dataFile.addFile(newDataFileName);
      objectOutputstreamTimes = 0;
    } catch (FileNotFoundException e) {
      LOG.error("Create data file failed.", e);
      throw new RuntimeException("create data file failed.", e);
    } catch (IOException e) {
      LOG.error("Create data file failed.", e);
      throw new RuntimeException("create data file failed.", e);
    }
  }

  private void write(Object m) {
    if (m == null) {
      return;
    }
    try {
      objectOutputstreamTimes++;
      if (objectOutputstreamTimes == 2000) {
        dataFileOutputStream.reset();
        objectOutputstreamTimes = 1;
      }
      dataFileOutputStream.writeObject(m);
      dataFileOutputStream.flush();
      (fileMap.get(meta.getFileWriting())).incrementAndGet();
    } catch (IOException e) {
      LOG.error("write message failed", e);
      throw new RuntimeException("write message failed", e);
    }

  }

  @Override
  public long size() {
    return meta.getUnReadCount();
  }

  public MemeoryQueueConfig getConfig() {
    return config;
  }

  public String getQueueName() {
    return queueName;
  }

  private static class MemCachedMessage {

    private Object data;
    private String file;
    private boolean isFirstMessage;
    private long pos;

    public MemCachedMessage(Object data, String file, boolean isFirstMessage, long pos) {
      this.data = data;
      this.file = file;
      this.isFirstMessage = isFirstMessage;
      this.pos = pos;
    }

    public long getPos() {
      return pos;
    }

    public Object getData() {
      return data;
    }

    public String getFile() {
      return file;
    }

    public boolean isFirstMessage() {
      return isFirstMessage;
    }

  }

}
