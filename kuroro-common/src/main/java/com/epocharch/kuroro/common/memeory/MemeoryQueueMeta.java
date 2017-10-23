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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemeoryQueueMeta {

  private static final Logger LOG = LoggerFactory.getLogger(MemeoryQueueMeta.class);
  private static final String STRING_NULL = "null";
  private final Meta meta = new Meta();
  private final String name = "meta";
  private final int maxFileLen = 2048;
  private final RandomAccessFile metaFile;
  private MappedByteBuffer mbb;

  public MemeoryQueueMeta(String queueName, MemeoryQueueConfig config) {
    String metaPath = (new StringBuilder(config.getMetaFilePath())).append(File.separator)
        .append(queueName).append(File.separator)
        .toString();
    if (!config.isNeedResume()) {
      File metaFile = new File(metaPath);
      if (metaFile.exists()) {
        FileUtils2.deleteQuietly(metaFile);
      }
    }
    ensureFile((new StringBuilder(metaPath)).append(name).toString());
    try {
      metaFile = new RandomAccessFile((new StringBuilder(metaPath)).append(name).toString(), "rwd");
      mbb = metaFile.getChannel()
          .map(java.nio.channels.FileChannel.MapMode.READ_WRITE, 0L, maxFileLen);
    } catch (Exception e) {
      throw new RuntimeException(
          (new StringBuilder("Construct ")).append(MemeoryQueueMeta.class.getCanonicalName())
              .append(" failed.").toString(), e);
    }
    loadFromDisk();
  }

  private void loadFromDisk() {
    mbb.position(0);
    int readingFileNameLen = mbb.getInt();
    long pos = mbb.getLong();
    byte readingFileNameByteArray[] = new byte[readingFileNameLen];
    try {
      mbb.get(readingFileNameByteArray);
    } catch (BufferUnderflowException e) {
      LOG.error("Meta file broken. Set position to 0 and Readingfile to null");
      meta.set(0L, "null");
      return;
    }
    int writingFileNameLen = mbb.getInt();
    byte writingFileNameByteArray[] = new byte[writingFileNameLen];
    try {
      mbb.get(writingFileNameByteArray);
    } catch (BufferUnderflowException e) {
      LOG.error("Meta file broken. Set Writingfile to null");
      meta.setFileWriting("null");
      return;
    }
    String readingFile = null;
    String writingFile = null;
    try {
      readingFile = new String(readingFileNameByteArray, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Processing readingFile name encoding failed.", e);
    }
    try {
      writingFile = new String(writingFileNameByteArray, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Processing writingFile name encoding failed.", e);
    }
    if (readingFile == null) {
      readingFile = STRING_NULL;
    }
    if (writingFile == null) {
      writingFile = STRING_NULL;
    }
    meta.setAddCount(mbb.getLong());
    meta.setConsumeCount(mbb.getLong());
    meta.set(pos, readingFile, writingFile);
  }

  private void ensureFile(String fileName) {
    File file = new File(fileName);
    if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
      throw new RuntimeException(
          (new StringBuilder("can not create dir: ")).append(file.getParent()).toString());
    }
    if (!file.exists()) {
      try {
        if (!file.createNewFile()) {
          throw new RuntimeException(
              (new StringBuilder("can not create file: ")).append(file).toString());
        }
      } catch (IOException e) {
        throw new RuntimeException(
            (new StringBuilder("can not create file: ")).append(file).toString());
      }
    }
  }

  public synchronized void updateRead(long pos, String fileReading) {
    try {
      meta.set(pos, fileReading);
      mbb.position(0);
      byte bytes[] = fileReading.getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.putLong(meta.getReadPos());
      mbb.put(bytes);
      bytes = meta.getFileWriting().getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.put(bytes);
      mbb.putLong(meta.getAddCount());
      mbb.putLong(meta.getConsumeCount());
    } catch (UnsupportedEncodingException e) {
      LOG.error("meta file UpdateRead failed.", e);
    }
  }

  public synchronized void incAddCount() {
    try {
      meta.setAddCount(meta.getAddCount() + 1L);
      mbb.position(0);
      byte bytes[] = meta.getFileReading().getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.putLong(meta.getReadPos());
      mbb.put(bytes);
      bytes = meta.getFileWriting().getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.put(bytes);
      mbb.putLong(meta.getAddCount());
      mbb.putLong(meta.getConsumeCount());
    } catch (UnsupportedEncodingException e) {
      LOG.error("meta file UpdateRead failed.", e);
    }
  }

  public synchronized void incConsumeCount() {
    try {
      meta.setConsumeCount(meta.getConsumeCount() + 1L);
      mbb.position(0);
      byte bytes[] = meta.getFileReading().getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.putLong(meta.getReadPos());
      mbb.put(bytes);
      bytes = meta.getFileWriting().getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.put(bytes);
      mbb.putLong(meta.getAddCount());
      mbb.putLong(meta.getConsumeCount());
    } catch (UnsupportedEncodingException e) {
      LOG.error("meta file UpdateRead failed.", e);
    }
  }

  public synchronized void updateWrite(String fileWriting) {
    try {
      meta.setFileWriting(fileWriting);
      mbb.position(12 + meta.getFileReading().getBytes().length);
      byte bytes[] = fileWriting.getBytes("UTF-8");
      mbb.putInt(bytes.length);
      mbb.put(bytes);
      mbb.putLong(meta.getAddCount());
      mbb.putLong(meta.getConsumeCount());
    } catch (UnsupportedEncodingException e) {
      LOG.error("meta file updateWrite failed.", e);
    }
  }

  public long getReadPos() {
    return meta.getReadPos();
  }

  public String getFileReading() {
    return meta.getFileReading();
  }

  public String getFileWriting() {
    return meta.getFileWriting();
  }

  public long getUnReadCount() {
    return meta.getAddCount() - meta.getConsumeCount();
  }

  public synchronized void close() {
    mbb.force();
    mbb = null;
    try {
      metaFile.close();
    } catch (IOException e) {
      LOG.error("Close metaFile failed.", e);
    }
  }

  public static class Meta {

    private volatile String fileWriting;
    private volatile String fileReading;
    private volatile long readPos;
    private volatile long addCount;
    private volatile long consumeCount;

    public Meta() {
      this(0L, "null", "null", 0L, 0L);
    }

    public Meta(long readPos) {
      this(readPos, "null", "null", 0L, 0L);
    }

    public Meta(long readPos, String fileReading) {
      this(readPos, fileReading, "null", 0L, 0L);
    }

    public Meta(long readPos, String fileReading, String fileWriting) {
      this(readPos, fileReading, fileWriting, 0L, 0L);
    }

    public Meta(long readPos, String fileReading, String fileWriting, long addCount,
        long consumeCount) {
      this.readPos = readPos;
      this.fileReading = fileReading;
      this.fileWriting = fileWriting;
      this.addCount = addCount;
      this.consumeCount = consumeCount;
    }

    public long getReadPos() {
      return readPos;
    }

    public String getFileWriting() {
      return fileWriting;
    }

    public void setFileWriting(String fileWriting) {
      this.fileWriting = fileWriting;
    }

    public String getFileReading() {
      return fileReading;
    }

    public long getAddCount() {
      return addCount;
    }

    public void setAddCount(long addCount) {
      this.addCount = addCount;
    }

    public long getConsumeCount() {
      return consumeCount;
    }

    public void setConsumeCount(long consumeCount) {
      this.consumeCount = consumeCount;
    }

    public void set(long readPos, String fileReading) {
      this.readPos = readPos;
      this.fileReading = fileReading;
    }

    public void set(long readPos, String fileReading, String fileWriting) {
      this.readPos = readPos;
      this.fileWriting = fileWriting;
      this.fileReading = fileReading;
    }

    public void set(long readPos) {
      this.readPos = readPos;
    }

  }

}
