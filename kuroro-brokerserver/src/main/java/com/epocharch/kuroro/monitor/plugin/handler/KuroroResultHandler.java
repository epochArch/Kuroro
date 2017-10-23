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

package com.epocharch.kuroro.monitor.plugin.handler;

import com.epocharch.kuroro.monitor.dao.impl.BaseMybatisDAOImpl;
import com.epocharch.kuroro.monitor.dto.KuroroConsumerAnalyse;
import com.epocharch.kuroro.monitor.dto.KuroroInOutAnalyse;
import com.epocharch.kuroro.monitor.dto.KuroroProducerAnalyse;
import com.epocharch.kuroro.monitor.plugin.ResultHandler;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * 测试批量接接口分析结果处理
 *
 * @author dongheng
 */
public class KuroroResultHandler implements ResultHandler {

  private static final Logger logger = Logger.getLogger(KuroroResultHandler.class);

  private BaseMybatisDAOImpl memoMyIbaitsDAO;

  public void setMemoMyIbaitsDAO(BaseMybatisDAOImpl memoMyIbaitsDAO) {
    this.memoMyIbaitsDAO = memoMyIbaitsDAO;
  }

  public void setAnalyseMyIbaitsDAO(
      BaseMybatisDAOImpl analyseMyIbaitsDAO) {
    this.analyseMyIbaitsDAO = analyseMyIbaitsDAO;
  }

  private BaseMybatisDAOImpl analyseMyIbaitsDAO;


  /**
   * 成功返回true, 失败返回false
   */
  @Override
  @SuppressWarnings("unchecked")
  public Object handle(Object object) {
    if (object != null) {
      List<KuroroProducerAnalyse> producerAnalyses = new ArrayList<KuroroProducerAnalyse>();
      List<KuroroConsumerAnalyse> consumerAnalyses = new ArrayList<KuroroConsumerAnalyse>();
      try {
        List<KuroroInOutAnalyse> list = (List<KuroroInOutAnalyse>) object;
        for (KuroroInOutAnalyse kuroroInOutAnalyse : list) {
          producerAnalyses.addAll(kuroroInOutAnalyse.getProducerAnalyses());
          consumerAnalyses.addAll(kuroroInOutAnalyse.getConsumerAnalyses());
        }
        if (producerAnalyses.size() > 0) {
          try {
            analyseMyIbaitsDAO
                .batchInsert("monitor_kuroro_producer_analyse.insert", producerAnalyses);
          } catch (Exception e) {
            e.printStackTrace();
            logger.error("analyzeIbaitsDAO  monitor_kuroro_producer_analyse.insert error.", e);
          }
          for (KuroroProducerAnalyse curt : producerAnalyses) {
            curt.setId(null);
          }
          try {
            memoMyIbaitsDAO.batchInsert("monitor_kuroro_producer_analyse.insert", producerAnalyses);
          } catch (Exception e) {
            e.printStackTrace();
            logger.error("memoIbaitsDAO  monitor_kuroro_producer_analyse.insert error.", e);
          }
        }
        if (consumerAnalyses.size() > 0) {
          try {
            memoMyIbaitsDAO.batchInsert("monitor_kuroro_consumer_analyse.insert", consumerAnalyses);
          } catch (Exception e) {
            e.printStackTrace();
            logger.error("memoIbaitsDAO.batchInsert(monitor_kuroro_consumer_analyse.insert", e);
          }

          for (KuroroConsumerAnalyse curt : consumerAnalyses) {
            curt.setId(null);
          }
          try {
            analyseMyIbaitsDAO
                .batchInsert("monitor_kuroro_consumer_analyse.insert", consumerAnalyses);
          } catch (Exception e) {
            e.printStackTrace();
            logger.error("analyzeIbaitsDAO.batchInsert(monitor_kuroro_consumer_analyse.insert", e);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("KuroroResultHandler insert error", e);
        return false;
      }
    }
    return true;
  }

}
