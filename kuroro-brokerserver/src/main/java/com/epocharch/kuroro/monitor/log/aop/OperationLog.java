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

package com.epocharch.kuroro.monitor.log.aop;

import java.io.Serializable;
import java.util.Date;

public class OperationLog implements Serializable  {

  private static final long serialVersionUID = -6737458328870011772L;
  private Long id;
  private String userName;
  private Date operationTime;//操作日期
  private Date operationTimeStart;//操作开始日期
  private Date operationTimeEnd;//操作结束日期
  private String ip;//操作者IP地址
  private String operationDetails;//操作详细信息
  private String moduleName;//用户所操作功能模块名
  private String operationName;//用户所做操作名
  private String resultCode;//操作结果码(0:成功、1:失败)
  private String idcName;//所在idc信息
  //分页参数
  private int start = 0;
  private int size = 0;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Date getOperationTime() {
    return operationTime;
  }

  public void setOperationTime(Date operationTime) {
    this.operationTime = operationTime;
  }

  public Date getOperationTimeStart() {
    return operationTimeStart;
  }

  public void setOperationTimeStart(Date operationTimeStart) {
    this.operationTimeStart = operationTimeStart;
  }

  public Date getOperationTimeEnd() {
    return operationTimeEnd;
  }

  public void setOperationTimeEnd(Date operationTimeEnd) {
    this.operationTimeEnd = operationTimeEnd;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getOperationDetails() {
    return operationDetails;
  }

  public void setOperationDetails(String operationDetails) {
    this.operationDetails = operationDetails;
  }

  public String getModuleName() {
    return moduleName;
  }

  public void setModuleName(String moduleName) {
    this.moduleName = moduleName;
  }

  public String getOperationName() {
    return operationName;
  }

  public void setOperationName(String operationName) {
    this.operationName = operationName;
  }

  public String getResultCode() {
    return resultCode;
  }

  public void setResultCode(String resultCode) {
    this.resultCode = resultCode;
  }

  public String getIdcName() {
    return idcName;
  }

  public void setIdcName(String idcName) {
    this.idcName = idcName;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((operationDetails == null) ? 0 : operationDetails.hashCode());
    result = prime * result + ((operationTime == null) ? 0 : operationTime.hashCode());
    result = prime * result + ((ip == null) ? 0 : ip.hashCode());
    result = prime * result + ((moduleName == null) ? 0 : moduleName.hashCode());
    result = prime * result + ((operationName == null) ? 0 : operationName.hashCode());
    result = prime * result + ((userName == null) ? 0 : userName.hashCode());
    result = prime * result + ((resultCode == null) ? 0 : resultCode.hashCode());
    result = prime * result + ((idcName == null) ? 0 : idcName.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "Log [idcName = " + idcName + ", userName = " + userName + ", operationTime = "
        + operationTime + ", ip = " + ip
        + ", moduleName = " + moduleName + ", operationName = " + operationName
        + ", operationDetails = " + operationDetails
        + ", resultCode = " + resultCode + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    OperationLog other = (OperationLog) obj;
    if (operationDetails == null) {
      if (other.operationDetails != null) {
        return false;
      }
    } else if (!operationDetails.equals(other.operationDetails)) {
      return false;
    }
    if (operationTime == null) {
      if (other.operationTime != null) {
        return false;
      }
    } else if (!operationTime.equals(other.operationTime)) {
      return false;
    }
    if (ip == null) {
      if (other.ip != null) {
        return false;
      }
    } else if (!ip.equals(other.ip)) {
      return false;
    }
    if (moduleName == null) {
      if (other.moduleName != null) {
        return false;
      }
    } else if (!moduleName.equals(other.moduleName)) {
      return false;
    }
    if (operationName == null) {
      if (other.operationName != null) {
        return false;
      }
    } else if (!operationName.equals(other.operationName)) {
      return false;
    }
    if (resultCode == null) {
      if (other.resultCode != null) {
        return false;
      }
    } else if (!resultCode.equals(other.resultCode)) {
      return false;
    }
    if (userName == null) {
      if (other.userName != null) {
        return false;
      }
    } else if (!userName.equals(other.userName)) {
      return false;
    }
    if (idcName == null) {
      if (other.idcName != null) {
        return false;
      }
    } else if (!idcName.equals(other.idcName)) {
      return false;
    }
    return true;
  }
}
