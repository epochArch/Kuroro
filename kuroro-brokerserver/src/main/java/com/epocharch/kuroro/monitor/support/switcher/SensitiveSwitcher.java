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

package com.epocharch.kuroro.monitor.support.switcher;

import com.alibaba.fastjson.JSONObject;
import com.epocharch.kuroro.monitor.support.BaseSwitcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SensitiveSwitcher extends BaseSwitcher {

  static private String sensitive = "SensitiveSwitcher";

  static private List<String> sensitiveServier = new ArrayList<String>();

  @Override
  public String regist() {
    sensitiveServier.add("roleService.delRole");
    sensitiveServier.add("roleService.updateRole");
    sensitiveServier.add("roleService.assignPvg");
    sensitiveServier.add("userService.delUsers");
    sensitiveServier.add("userService.userUpdate");
    return sensitive;
  }

  @Override
  public Object logic(Map<String, String[]> params) {
    JSONObject result = new JSONObject();

    String[] method = params.get("m");
    String[] servername = params.get("c");
    if (method != null && servername != null) {
      if ("add".equals(method[0])) {
        for (String str : servername) {
          sensitiveServier.add(str);
        }
        result.put("result", "success");
        result.put("info", JSONObject.toJSONString(sensitiveServier));
        return result;
      } else if ("del".equals(method[0])) {
        for (String str : servername) {
          sensitiveServier.remove(str);
        }
        result.put("result", "success");
        result.put("info", JSONObject.toJSONString(sensitiveServier));
        return result;
      }
    }
    result.put("result", "fail");
    result.put("info", "param is null or param is false");
    return result;
  }

  @Override
  public String urlDesc() {
    return "flog.do?switch=SensitiveSwitcher&m=add|del&c=m";
  }

  @Override
  public Object status() {
    return sensitiveServier;
  }

  public boolean contains(String str) {
    return sensitiveServier.contains(str);
  }

}
