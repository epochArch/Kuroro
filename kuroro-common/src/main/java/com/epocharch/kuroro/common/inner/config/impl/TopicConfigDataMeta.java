package com.epocharch.kuroro.common.inner.config.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TopicConfigDataMeta implements Serializable {

  private static final long serialVersionUID = -8356797072772863678L;
  private String topicName;
  private Integer messageCappedSize;
  @Deprecated
  private Integer messageCappedMaxDocNum;
  private Integer ackCappedSize;
  @Deprecated
  private Integer ackCappedMaxDocNum;
  private String type;
  private Boolean compensate;
  @Deprecated
  private Integer weight;
  private Integer level;
  @Deprecated
  private Integer partion;
  private Long timeStamp;
  private List<String> replicationSetList;

  // 联系人信息
  private String poolName;
  private String ownerEmail;
  private String ownerName;
  private String ownerPhone;
  private String comment;
  // 附加信息
  @Deprecated
  private String alarmParams;
  @Deprecated
  // 写入可靠级别
  private Integer reliability;
  // broker分组名称
  private String brokerGroup;
  //mongo group
  private String mongoGroup;
  //是否允许生产者发送消息   默认允许
  private Boolean sendFlag = true;
  //consumer flag
  private Boolean consumerFlowControl = true;
  //flow control size
  private Integer flowSize;

  private List<String> suspendConsumerIdList = new ArrayList<String>();

  private ConcurrentHashMap<String, Integer> flowConsumerIdMap = new ConcurrentHashMap<String, Integer>();

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public Integer getMessageCappedSize() {
    return messageCappedSize;
  }

  public void setMessageCappedSize(Integer messageCappedSize) {
    this.messageCappedSize = messageCappedSize;
  }

  public Integer getMessageCappedMaxDocNum() {
    return messageCappedMaxDocNum;
  }

  public void setMessageCappedMaxDocNum(Integer messageCappedMaxDocNum) {
    this.messageCappedMaxDocNum = messageCappedMaxDocNum;
  }

  public Integer getAckCappedSize() {
    return ackCappedSize;
  }

  public void setAckCappedSize(Integer ackCappedSize) {
    this.ackCappedSize = ackCappedSize;
  }

  public Integer getAckCappedMaxDocNum() {
    return ackCappedMaxDocNum;
  }

  public void setAckCappedMaxDocNum(Integer ackCappedMaxDocNum) {
    this.ackCappedMaxDocNum = ackCappedMaxDocNum;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Integer getWeight() {
    return weight;
  }

  public void setWeight(Integer weight) {
    this.weight = weight;
  }

  public List<String> getReplicationSetList() {
    return replicationSetList;
  }

  public void setReplicationSetList(List<String> replicationSetList) {
    this.replicationSetList = replicationSetList;
  }

  public Integer getLevel() {
    return level;
  }

  public void setLevel(Integer level) {
    this.level = level;
  }

  public Long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(Long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public Integer getPartion() {
    return partion;
  }

  public void setPartion(Integer partion) {
    this.partion = partion;
  }

  public Boolean isCompensate() {
    return compensate;
  }

  public void setCompensate(Boolean compensate) {
    this.compensate = compensate;
  }

  public String getPoolName() {
    return poolName;
  }

  public void setPoolName(String poolName) {
    this.poolName = poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public String getOwnerPhone() {
    return ownerPhone;
  }

  public void setOwnerPhone(String ownerPhone) {
    this.ownerPhone = ownerPhone;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getOwnerEmail() {
    return ownerEmail;
  }

  public void setOwnerEmail(String ownerEmail) {
    this.ownerEmail = ownerEmail;
  }

  public String getAlarmParams() {
    return alarmParams;
  }

  public void setAlarmParams(String alarmParams) {
    this.alarmParams = alarmParams;
  }

  public Integer getReliability() {
    return reliability;
  }

  public void setReliability(Integer reliability) {
    this.reliability = reliability;
  }

  public String getBrokerGroup() {
    return brokerGroup;
  }

  public void setBrokerGroup(String brokerGroup) {
    this.brokerGroup = brokerGroup;
  }

  public String getMongoGroup() {
    return mongoGroup;
  }

  public void setMongoGroup(String mongoGroup) {
    this.mongoGroup = mongoGroup;
  }

  public Boolean isSendFlag() {
    return sendFlag;
  }

  public void setSendFlag(Boolean sendFlag) {
    this.sendFlag = sendFlag;
  }

  @Override
  public String toString() {
    return "TopicConfigDataMeta{" + "topicName='" + topicName + '\'' + ", messageCappedSize="
        + messageCappedSize + ", flowSize="
        + flowSize + ", messageCappedMaxDocNum=" + messageCappedMaxDocNum + ", ackCappedSize="
        + ackCappedSize
        + ", ackCappedMaxDocNum=" + ackCappedMaxDocNum + ", type='" + type + '\'' + ", compensate="
        + compensate + ", weight="
        + weight + ", level=" + level + ", partion=" + partion + ", timeStamp=" + timeStamp
        + ", replicationSetList="
        + replicationSetList + ", poolName='" + poolName + '\'' + ", ownerEmail='" + ownerEmail
        + '\'' + ", ownerName='" + ownerName
        + '\'' + ", ownerPhone='" + ownerPhone + '\'' + ", comment='" + comment + '\''
        + ", alarmParams='" + alarmParams + '\''
        + ", brokerGroup='" + brokerGroup + '\'' + ", mongoGroup='" + mongoGroup + '\''
        + ", sendFlag=" + sendFlag + '\''
        + ", consumerFlowControl=" + consumerFlowControl + '\'' + ", flowConsumerIdMap="
        + flowConsumerIdMap + ", reliability="
        + reliability + '}';
  }

  public List<String> getSuspendConsumerIdList() {
    return suspendConsumerIdList;
  }

  public void setSuspendConsumerIdList(List<String> suspendConsumerIdList) {
    this.suspendConsumerIdList = suspendConsumerIdList;
  }

  public Boolean isConsumerFlowControl() {
    return consumerFlowControl;
  }

  public void setConsumerFlowControl(Boolean consumerFlowControl) {
    this.consumerFlowControl = consumerFlowControl;
  }

  public Integer getFlowSize() {
    return flowSize;
  }

  public void setFlowSize(Integer flowSize) {
    this.flowSize = flowSize;
  }

  public ConcurrentHashMap<String, Integer> getFlowConsumerIdMap() {
    return flowConsumerIdMap;
  }

  public void setFlowConsumerIdMap(ConcurrentHashMap<String, Integer> flowConsumerIdMap) {
    this.flowConsumerIdMap = flowConsumerIdMap;
  }

}
