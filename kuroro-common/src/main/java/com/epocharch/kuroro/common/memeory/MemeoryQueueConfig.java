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

public class MemeoryQueueConfig {

  private final String DEFAULT_PREFIX = "/var/www/data/filequeue";
  private final String DEFAULT_METAFILEPATH = "/meta";
  private final String DEFAULT_DATAFILEPATH = "/data";
  private final String DEFAULT_DATABAKPATH = "/bak";
  private final int DEFAULT_MAXDATAFILESIZE = 0x6400000;
  private final int DEFAULT_MEMCACHESIZE = 10000;
  private String metaFilePath;
  private String dataFilePath;
  private long maxDataFileSize;
  private String dataBakPath;
  private boolean dataBakOn;
  private String configName;
  private int memeoryMaxSize;
  private boolean isNeedResume;

  public MemeoryQueueConfig() {
    String prefix = System.getProperty("kuroro.tmp", DEFAULT_PREFIX);
    this.metaFilePath = prefix + DEFAULT_METAFILEPATH;
    this.dataFilePath = prefix + DEFAULT_DATAFILEPATH;
    this.dataBakPath = prefix + DEFAULT_DATABAKPATH;
    this.maxDataFileSize = DEFAULT_MAXDATAFILESIZE;
    this.memeoryMaxSize = DEFAULT_MEMCACHESIZE;
    this.configName = "default";
    this.isNeedResume = false;
    this.dataBakOn = false;
  }

  public boolean isNeedResume() {
    return isNeedResume;
  }

  public void setNeedResume(boolean isNeedResume) {
    this.isNeedResume = isNeedResume;
  }

  public int getMemeoryMaxSize() {
    return memeoryMaxSize;
  }

  public void setMemeoryMaxSize(int memeoryMaxSize) {
    this.memeoryMaxSize = memeoryMaxSize;
  }

  public String getMetaFilePath() {
    return metaFilePath;
  }

  public void setMetaFilePath(String metaFilePath) {
    this.metaFilePath = metaFilePath;
  }

  public String getDataFilePath() {
    return dataFilePath;
  }

  public void setDataFilePath(String dataFilePath) {
    this.dataFilePath = dataFilePath;
  }

  public long getMaxDataFileSize() {
    return maxDataFileSize;
  }

  public void setMaxDataFileSize(long maxDataFileSize) {
    this.maxDataFileSize = maxDataFileSize;
  }

  public String getDataBakPath() {
    return dataBakPath;
  }

  public void setDataBakPath(String dataBakPath) {
    this.dataBakPath = dataBakPath;
  }

  public boolean isDataBakOn() {
    return dataBakOn;
  }

  public void setDataBakOn(boolean dataBakOn) {
    this.dataBakOn = dataBakOn;
  }

  public String getConfigName() {
    return configName;
  }

  public void setConfigName(String configName) {
    this.configName = configName;
  }
}
