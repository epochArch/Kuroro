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
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemeoryQueueDataFile {

  private static final Logger LOG = LoggerFactory.getLogger(MemeoryQueueDataFile.class);
  private final String path;
  private final String prefix = "defaultfilequeue";
  private final TreeSet<String> files = new TreeSet<String>();
  private final TreeSet<String> oldFiles = new TreeSet<String>();
  private final String queueName;
  private final MemeoryQueueConfig memeoryQueueConfig;

  public MemeoryQueueDataFile(String queuename, MemeoryQueueConfig config) {
    this.queueName = queuename;
    this.memeoryQueueConfig = config;
    path = (new StringBuilder(config.getDataFilePath())).append(File.separator).append(queueName)
        .append(File.separator).toString();
    if (!memeoryQueueConfig.isNeedResume()) {
      clean();
    }
    check();
  }

  private void clean() {
    File dir = new File(path);
    if (dir.exists()) {
      FileUtils2.deleteQuietly(dir);
    }
  }

  private void check() {
    File dir = new File(path);
    if (!dir.exists()) {
      return;
    }
    String[] nameList = dir.list(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix);
      }
    });
    for (String fileName : nameList) {
      String fullName = (new StringBuilder(path)).append(fileName).toString();
      files.add(fullName);
    }
  }

  public synchronized String createQueueFile() {
    Long count = 0L;
    if (!files.isEmpty()) {
      String lastFileName = files.last();
      count =
          Long.valueOf(lastFileName.substring(lastFileName.lastIndexOf(".") + 1)).longValue() + 1L;
    } else {
      if (!oldFiles.isEmpty()) {
        String lastFileName = oldFiles.last();
        count = Long.valueOf(lastFileName.substring(lastFileName.lastIndexOf(".") + 1)).longValue()
            + 1L;
      }
    }
    ensureDir(path);
    String newFileName = (new StringBuilder(String.valueOf(path))).append(prefix).append(".")
        .append(String.format("%019d", new Object[]{Long.valueOf(count)})).toString();
    // 不能在这里把新的文件名添加到files，因为文件还没有真正创建，导致读线程抛异常
    // files.add(newFileName);
    return newFileName;
  }

  private void ensureDir(String path) {
    File baseFile = new File(path);
    if (!baseFile.exists() && !baseFile.mkdirs()) {
      throw new RuntimeException(
          (new StringBuilder("Can not create dir: ")).append(path).toString());
    } else {
      return;
    }
  }

  public synchronized String getQueueFileName() {
    if (files.size() > 0) {
      String fullname = files.first();
      files.remove(fullname);
      if (fullname != null) {
        oldFiles.add(fullname);
      }
      return fullname;
    } else {
      return null;
    }
  }

  public synchronized void adjust(String fileReading) {
    Iterator<String> iterator = files.iterator();
    while (iterator.hasNext()) {
      String file = iterator.next();
      if (Long.valueOf(fileReading.substring(fileReading.lastIndexOf(".") + 1)).longValue() >= Long
          .valueOf(file.substring(file.lastIndexOf(".") + 1)).longValue()) {
        // files.remove(file);
        iterator.remove();
        oldFiles.add(file);
      }
    }
  }

  public synchronized void archiveAndRemoveOldFile(String filePath) {
    if (oldFiles.size() == 0) {
      return;
    }
    String first = oldFiles.first();
    if (first.equals(filePath)) {
      return;
    }
    Set<String> subSet = oldFiles.subSet(first, filePath);
    Iterator<String> iterator = subSet.iterator();
    while (iterator.hasNext()) {
      String toDelFileName = iterator.next();
      if (!toDelFileName.equals(filePath)) {
        File toDel = new File(toDelFileName);
        if (memeoryQueueConfig.isDataBakOn()) {
          SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
          String dataBakPath = memeoryQueueConfig.getDataBakPath();
          String bakPath = (new StringBuilder(String.valueOf(dataBakPath))).append(File.separator)
              .append(queueName)
              .append(File.separator).append(sf.format(new Date())).append(File.separator)
              .toString();
          ensureDir(bakPath);
          try {
            FileUtils.copyFileToDirectory(toDel, new File(bakPath));
          } catch (IOException e) {
            LOG.error(e.getMessage());
          }
        }
        if (!toDel.delete()) {
          LOG.error((new StringBuilder(String.valueOf(toDelFileName))).append(" delete failed.")
              .toString());
        } else {
          LOG.info("file:" + toDelFileName + " is deleted ok");
          // oldFiles.remove(toDelFileName);
          iterator.remove();
        }
      }
    }
  }

  public synchronized boolean isEmpty() {
    return files.isEmpty();
  }

  public synchronized boolean addFile(String fileName) {
    return files.add(fileName);
  }
}
