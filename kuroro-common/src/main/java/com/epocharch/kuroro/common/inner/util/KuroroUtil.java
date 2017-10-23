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

package com.epocharch.kuroro.common.inner.util;

import com.epocharch.common.config.PropertiesContainer;
import com.epocharch.common.constants.DeployLevel;
import com.epocharch.common.zone.zk.ZkZoneContainer;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.zkclient.ZkClient;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhoufeiqiang on 28/09/2017.
 */
public class KuroroUtil {

  private static final Logger logger = LoggerFactory.getLogger(KuroroUtil.class);
  private static String urlEnd = Constants.PRODUCTION.toLowerCase() + "/ajax.do";
  private static Properties openAPI = new Properties();

  public static boolean isBlankString(String value) {
    return value == null || "".equals(value.trim());
  }

  public static boolean isEmptyList(List list) {
    return list == null || list.size() == 0;
  }

  public static boolean isBlankObject(Object value) {
    return value == null || "".equals(value);
  }

  //send Email
  public static void sendEmail(String emails, String content, String object) {

  }

  public static String currentOpenAPI(String idcName) {
    String value = null;
    String localEnv = PropertiesContainer.getInstance().getProperty(InternalPropKey.CURRENT_ENV);
    if (KuroroUtil.isBlankString(localEnv)) {
      localEnv = "default";
      throw new NullPointerException("idcName is not null!");
    }
    value = loadZkOpenAPI(idcName);
    if (!KuroroUtil.isBlankString(value)) {
      value = value + urlEnd;
    }
    return value;
  }

  private static String loadZkOpenAPI(String idcName) {
    String openApiPath =
        System.getProperty(InternalPropKey.IDC_OPEN_API_NAME) + Constants.SEPARATOR + idcName;
    ZkClient idcZk = KuroroZkUtil.initIDCZk();
    String value = null;
    if (idcZk.exists(openApiPath)) {
      value = idcZk.readData(openApiPath);
    }
    return value;
  }

  public static boolean isIdcProduction() {
    boolean isProduction = false;
    DeployLevel level = ZkZoneContainer.getInstance().getLevel();
    if (!level.equals(DeployLevel.ZONE)) {
      isProduction = true;
    }
    return isProduction;
  }

  public static String poolIdName() {
    String poolId = PropertiesContainer.getInstance().getProperty(InternalPropKey.POOIDNAME);
    if (!KuroroUtil.isBlankString(poolId)) {
      if (!KuroroUtil.isBlankString(poolId)) {
        return poolId.replace("/", "_");
      }
    }
    return null;
  }

  public static String getChildFullPath(String parentPath, String shortChildPath) {
    return parentPath + "/" + shortChildPath;
  }

  public static String getChildShortPath(String fullPath) {
    return fullPath.substring(fullPath.lastIndexOf("/") + 1);
  }
}
