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

import com.epocharch.kuroro.monitor.dao.BaseMybatisDAO;

public class OperationLogService {

  private BaseMybatisDAO analyseMyIbaitsDAO;

  public void setAnalyseMyIbaitsDAO(BaseMybatisDAO analyseMyIbaitsDAO) {
    this.analyseMyIbaitsDAO = analyseMyIbaitsDAO;
  }

/*
  public Page<OperationLog> getOperationLogByPage(OperationLog operationLog) {
    Page<OperationLog> pageResult = new Page<OperationLog>();
    Integer totalCount = baseIbaitsDAO
        .queryForEntity("monitor_operation_log.getLogPageCount", operationLog, Integer.class);
    pageResult.setTotalCount(totalCount);
    List<OperationLog> operationLogs = baseIbaitsDAO
        .queryForList("monitor_operation_log.getLogForPage", operationLog, OperationLog.class);
    pageResult.setResult(operationLogs);
    return pageResult;
  }*/
}
