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

package com.epocharch.kuroro.common.jmx.support;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import javax.management.Descriptor;
import javax.management.IntrospectionException;
import javax.management.modelmbean.ModelMBeanAttributeInfo;

public class MBeanAttributeInfoAssembler {

  public static final String METHOD_GET = "getMethod";
  public static final String METHOD_SET = "setMethod";
  private final PropertyDescriptor descriptor;

  public MBeanAttributeInfoAssembler(PropertyDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  /*
   * get bean attr
   */
  public ModelMBeanAttributeInfo getMBeanAttrInfo() throws IntrospectionException {
    Method getter = descriptor.getReadMethod();
    if (getter != null && getter.getDeclaringClass() == Object.class) {
      // getClass
      return null;
    }
    Method setter = descriptor.getWriteMethod();
    if (getter != null || setter != null) {
      String attrName = firstCapitalize(descriptor.getName());
      String description = descriptor.getDisplayName();
      ModelMBeanAttributeInfo attrInfo = new ModelMBeanAttributeInfo(attrName, description, getter,
          setter);
      Descriptor desc = attrInfo.getDescriptor();
      if (getter != null) {
        desc.setField(METHOD_GET, getter.getName());
      }
      if (setter != null) {
        desc.setField(METHOD_SET, setter.getName());
      }
      attrInfo.setDescriptor(desc);
      return attrInfo;
    }
    return null;
  }

  private String firstCapitalize(String str) {
    if (str == null || str.length() == 0) {
      return str;
    }
    StringBuilder buf = new StringBuilder(str.length());
    buf.append(Character.toUpperCase(str.charAt(0)));
    buf.append(str.substring(1));
    return buf.toString();
  }
}
