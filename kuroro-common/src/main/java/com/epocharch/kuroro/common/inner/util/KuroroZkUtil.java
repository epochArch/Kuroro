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

import com.epocharch.common.constants.ClusterUsage;
import com.epocharch.common.zone.Zone;
import com.epocharch.common.zone.zk.ZkZoneContainer;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.zkclient.ZkClient;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhoufeiqiang on 28/09/2017.
 */
public class KuroroZkUtil {

  public static String localZone() {

    String localZone = System.getProperty(InternalPropKey.ZONE_LOCALZONE);
    if (KuroroUtil.isBlankString(localZone)) {
      localZone = ZkZoneContainer.getInstance().getLocalZoneName();
      if (KuroroUtil.isBlankString(localZone)) {
        localZone = "UnKnownZoneName";
        throw new NullPointerException("---can not find zone name!---");
      }
    }
    return localZone;
  }

  public static String localIDCName() {
    String localIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    if (KuroroUtil.isBlankString(localIDCName)) {
      localIDCName = ZkZoneContainer.getInstance().getIdcContainer().getLocalIdc();
      if (KuroroUtil.isBlankString(localIDCName)) {
        localIDCName = "UnKnownIdcName";
        throw new NullPointerException("---can not find localIdcName!---");
      }
    }
    return localIDCName;
  }

  public static Set<String> allIDCName() {
    Set<String> idcList = ZkZoneContainer.getInstance().getIdcContainer().getAllIdc();
    return idcList;
  }

  public static ZkClient initLocalMqZk() {
    ZkClient zkClient = ZkZoneContainer.getInstance().getLocalZkClient(ClusterUsage.SOA);

    return zkClient;
  }

  public static ZkClient initMqZK(String zone) {
    ZkClient zkClient = null;
    try {
      zkClient = ZkZoneContainer.getInstance().getZkClient(zone, ClusterUsage.MQ);
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    return zkClient;
  }

  public static ZkClient initZK(String zone, ClusterUsage usage) {
    ZkClient zkClient = null;
    try {
      zkClient = ZkZoneContainer.getInstance().getZkClient(zone, usage);
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    return zkClient;
  }

  //client init
  public static void initClientIDCMetas() {
    String localZoneName = localZone();
    String localIDCName = localIDCName();
    String zoneZkRootPath =
        Constants.ZONE_ZK_PARENT_ROOT + Constants.SEPARATOR + localIDCName + Constants.PRODUCTION
            + Constants.ZONES;
    String IDCZkRootPath =
        Constants.ZONE_ZK_PARENT_ROOT + Constants.SEPARATOR + localIDCName + Constants.PRODUCTION;

    System.setProperty(InternalPropKey.ZONE_LOCALZONE, localZoneName);
    System.setProperty(InternalPropKey.ZONE_IDCNAME, localIDCName);
    System.setProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH, zoneZkRootPath);
    System.setProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH, IDCZkRootPath);
  }

  //broker init
  public static void initBrokerIDCMetas() {
    String localIDCName = localIDCName();
    String localZoneName = localZone();
    String IDCZkRootPath =
        Constants.ZONE_ZK_PARENT_ROOT + Constants.SEPARATOR + localIDCName + Constants.PRODUCTION;
    String openApi = Constants.ZONE_ZK_PARENT_ROOT + Constants.IDC_OPEAPI;
    System.setProperty(InternalPropKey.ZONE_IDCNAME, localIDCName);
    System.setProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH, IDCZkRootPath);
    System.setProperty(InternalPropKey.ZONE_LOCALZONE, localZoneName);
    System.setProperty(InternalPropKey.IDC_OPEN_API_NAME, openApi);
  }

  public static ZkClient initIDCZk() {
    ZkClient _zkClient = ZkZoneContainer.getInstance().getLocalZkClient(ClusterUsage.SOA);
    return _zkClient;
  }

  public static ZkClient initURLZk(String url) {
    ZkClient _zkClient = new ZkClient(url);
    return _zkClient;
  }

  public static Set<String> localIdcZones() {
    String idcName = localIDCName();
    String localZoneName = localZone();
    Set<Zone> zones = ZkZoneContainer.getInstance().getIdcContainer().getIdcZones(idcName);
    Set<String> zoneName = new HashSet<String>();
    if (zones == null || zones.size() == 0) {
      return zoneName;
    }
    for (Zone zone : zones) {
      if (zones.size() == 1) {
        zoneName.add(zone.getName());
      } else if (zones.size() > 1 && !zone.getName().equals(localZoneName)) {
        zoneName.add(zone.getName());
      }
    }
    return zoneName;
  }

  public static Set<String> localIdcAllZones() {
    String idcName = localIDCName();
    Set<Zone> zones = ZkZoneContainer.getInstance().getIdcContainer().getIdcZones(idcName);
    Set<String> zoneName = new HashSet<String>();
    if (zones == null || zones.size() == 0) {
      return zoneName;
    }
    for (Zone zone : zones) {
      zoneName.add(zone.getName());
    }
    return zoneName;
  }

  public static Set<String> allIdcZoneName() {
    Set<String> allIdcZones = ZkZoneContainer.getInstance().getAllZoneName();
    return allIdcZones;
  }
}
