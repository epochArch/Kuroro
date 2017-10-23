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

package com.epocharch.kuroro.monitor.dto;

import java.io.Serializable;

/**
 * Created by zhoufeiqiang on 15/07/2017.
 */
public class MessageView implements Serializable, Cloneable {

  private static final long serialVersionUID = 9146843857520113300L;

  /*
    *
    * producer field
    *
    * */
  private int pTimeStamp;

  private int pInc;

  private String pContent;

  private Object pObjectContent;

  private String pCreatedTime;

  private String pSourceIp;

  private String pProtocolType;

  private String pType;

  private String pZone;

  private String pPoolId;

  private String pCompression;



    /*
  * consumer field
    *
    * */

  private int cTimeStamp;

  private int cInc;

  private String cSourceIp;

  private String cConsumerIp;

  private String cConsumerTime;

  private String cZone;

  private String cPoolId;

  public int getPTimeStamp() {
    return pTimeStamp;
  }

  public void setPTimeStamp(int pTimeStamp) {
    this.pTimeStamp = pTimeStamp;
  }

  public int getPInc() {
    return pInc;
  }

  public void setPInc(int pInc) {
    this.pInc = pInc;
  }

  public String getPContent() {
    return pContent;
  }

  public void setPContent(String pContent) {
    this.pContent = pContent;
  }

  public Object getPObectContent() {
    return pObjectContent;
  }

  public void setPObectContent(Object pObjectContent) {
    this.pObjectContent = pObjectContent;
  }

  public String getPCreatedTime() {
    return pCreatedTime;
  }

  public void setPCreatedTime(String pCreatedTime) {
    this.pCreatedTime = pCreatedTime;
  }

  public String getPSourceIp() {
    return pSourceIp;
  }

  public void setPSourceIp(String pSourceIp) {
    this.pSourceIp = pSourceIp;
  }

  public String getPProtocolType() {
    return pProtocolType;
  }

  public void setPProtocolType(String pProtocolType) {
    this.pProtocolType = pProtocolType;
  }

  public String getPType() {
    return pType;
  }

  public void setPType(String pType) {
    this.pType = pType;
  }

  public String getPZone() {
    return pZone;
  }

  public void setPZone(String pZone) {
    this.pZone = pZone;
  }

  public String getPPoolId() {
    return pPoolId;
  }

  public void setPPoolId(String pPoolId) {
    this.pPoolId = pPoolId;
  }

  public String getPCompression() {
    return pCompression;
  }

  public void setPCompression(String pCompression) {
    this.pCompression = pCompression;
  }

  public int getCTimeStamp() {
    return cTimeStamp;
  }

  public void setCTimeStamp(int cTimeStamp) {
    this.cTimeStamp = cTimeStamp;
  }

  public int getCInc() {
    return cInc;
  }

  public void setCInc(int cInc) {
    this.cInc = cInc;
  }

  public String getCSourceIp() {
    return cSourceIp;
  }

  public void setCSourceIp(String cSourceIp) {
    this.cSourceIp = cSourceIp;
  }

  public String getCConsumerIp() {
    return cConsumerIp;
  }

  public void setCConsumerIp(String cConsumerIp) {
    this.cConsumerIp = cConsumerIp;
  }

  public String getCZone() {
    return cZone;
  }

  public void setCZone(String cZone) {
    this.cZone = cZone;
  }

  public String getCPoolId() {
    return cPoolId;
  }

  public void setCPoolId(String cPoolId) {
    this.cPoolId = cPoolId;
  }

  public String getcConsumerTime() {
    return cConsumerTime;
  }

  public void setcConsumerTime(String cConsumerTime) {
    this.cConsumerTime = cConsumerTime;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
