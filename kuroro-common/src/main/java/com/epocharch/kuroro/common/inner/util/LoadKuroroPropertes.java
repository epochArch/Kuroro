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

import com.epocharch.kuroro.common.constants.InConstants;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhoufeiqiang on 28/04/2017.
 */
public class LoadKuroroPropertes {

  private static Properties mqProperties = new Properties();
  private static LoadKuroroPropertes loadKuroroPropertes;
  private Logger logger = LoggerFactory.getLogger(LoadKuroroPropertes.class);

  public LoadKuroroPropertes() {

  }

  public static synchronized LoadKuroroPropertes provider() {
    if (loadKuroroPropertes == null) {
      loadKuroroPropertes = new LoadKuroroPropertes();
      loadKuroroPropertes.loadFromClassPath();
    }
    return loadKuroroPropertes;
  }

  public void loadFromClassPath() {
    InputStream input = null;
    Properties p = new Properties();
    String configPath = InConstants.KURORO_POOL_CONFIG_NAME;
    ClassLoader cloader = this.getClass().getClassLoader();
    URL url = cloader.getSystemResource(configPath);
    try {
      if (url != null) {
        input = url.openStream();
      } else {
        input = cloader.getSystemResourceAsStream(configPath);
        if (input == null) {
          input = cloader.getResourceAsStream(configPath);
        }
      }
      if (input != null) {
        p.load(input);
        if (!p.isEmpty()) {
          mqProperties.putAll(p);
        }
      }
    } catch (IOException e) {
      logger.error("load classpath is fail! ", e);
      System.exit(1);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }


  public InputStream loadFileFromClasspath(String filePath) throws IOException {
    InputStream input = null;
    ClassLoader clzLoader = this.getClass().getClassLoader();
    URL url = clzLoader.getSystemResource(filePath);

    if (url != null) {
      input = url.openStream();
    } else {
      input = clzLoader.getSystemResourceAsStream(filePath);

			/*
			 * Classpath based resource could be loaded by this way in j2ee environment.
			 */
      if (input == null) {
        input = clzLoader.getResourceAsStream(filePath);
      }
    }
    return input;
  }

  public String getProperty(String key) {
    String value = mqProperties.getProperty(key);
    return value;
  }

  public int getIntProperty(String key) {
    String value = mqProperties.getProperty(key);
    int v = 0;
    if (value != null) {
      v = Integer.valueOf(value);
    }
    return v;
  }
}
