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

package com.epocharch.kuroro.common.hash;

import com.epocharch.kuroro.common.constants.Constants;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Archer Jiang
 */
public class HashFunctionFactory {

  private static HashFunctionFactory hfFactory = new HashFunctionFactory();

  public Map<String, HashFunction> functionMap = new HashMap<String, HashFunction>();

  private HashFunctionFactory() {
    super();
    functionMap.put(Constants.HASH_FUNCTION_MUR2, new Murmur2());
  }

  public static HashFunctionFactory getInstance() {
    return hfFactory;
  }

  public HashFunction getMur2Function() {
    HashFunction f = null;
    try {
      f = getHashFunction(Constants.HASH_FUNCTION_MUR2);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return f;
  }

  public HashFunction getHashFunction(String key) throws Exception {
    if (key == null) {
      throw new NullPointerException("Hash function key must not null!!!");
    }
    if (functionMap.containsKey(key)) {
      return functionMap.get(key);
    } else {
      throw new RuntimeException("Hash function key:" + key + " is not support");
    }
  }
}
