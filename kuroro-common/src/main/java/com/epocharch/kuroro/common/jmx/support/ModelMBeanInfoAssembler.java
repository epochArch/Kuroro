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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.management.JMException;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanConstructorInfo;
import javax.management.modelmbean.ModelMBeanInfo;
import javax.management.modelmbean.ModelMBeanInfoSupport;
import javax.management.modelmbean.ModelMBeanNotificationInfo;
import javax.management.modelmbean.ModelMBeanOperationInfo;

public class ModelMBeanInfoAssembler implements MBeanInfoAssembler {

  @Override
  public ModelMBeanInfo getMBeanInfo(Object managedBean, String key) throws JMException {
    return new ModelMBeanInfoSupport(managedBean.getClass().getName(),
        managedBean.getClass().getName() + " instance",
        getAttributeInfo(managedBean, key), new ModelMBeanConstructorInfo[0],
        getOperationInfo(managedBean, key),
        new ModelMBeanNotificationInfo[0]);
  }

  private ModelMBeanAttributeInfo[] getAttributeInfo(Object managedBean, String key)
      throws JMException {
    List<ModelMBeanAttributeInfo> attrInfoList = new ArrayList<ModelMBeanAttributeInfo>();
    try {
      BeanInfo beanInfo = Introspector.getBeanInfo(managedBean.getClass());
      PropertyDescriptor[] propDescriptors = beanInfo.getPropertyDescriptors();
      for (PropertyDescriptor propDescriptor : propDescriptors) {
        ModelMBeanAttributeInfo attrInfo = new MBeanAttributeInfoAssembler(propDescriptor)
            .getMBeanAttrInfo();
        if (attrInfo != null) {
          attrInfoList.add(attrInfo);
        }
      }
      Collections.sort(attrInfoList, new Comparator<ModelMBeanAttributeInfo>() {
        @Override
        public int compare(ModelMBeanAttributeInfo o1, ModelMBeanAttributeInfo o2) {
          return o1.getName().compareTo(o2.getName());
        }
      });
    } catch (IntrospectionException e) {
      throw new JMException(
          "Introspect BeanInfo for bean[" + managedBean + "] failed, cause:\r\n" + e);
    }
    return attrInfoList.toArray(new ModelMBeanAttributeInfo[attrInfoList.size()]);
  }

  private ModelMBeanOperationInfo[] getOperationInfo(Object managedBean, String beanKey)
      throws JMException {
    List<ModelMBeanOperationInfo> opInfoList = new ArrayList<ModelMBeanOperationInfo>();
    Method[] methods = managedBean.getClass().getMethods();
    try {
      for (Method method : methods) {
        ModelMBeanOperationInfo opInfo = new MBeanOperationInfoAssembler(managedBean, method)
            .getMBeanOperationInfo();
        if (opInfo != null) {
          opInfoList.add(opInfo);
        }
      }
      Collections.sort(opInfoList, new Comparator<ModelMBeanOperationInfo>() {
        @Override
        public int compare(ModelMBeanOperationInfo o1, ModelMBeanOperationInfo o2) {
          return o1.getName().compareTo(o2.getName());
        }
      });
    } catch (IntrospectionException e) {
      throw new JMException(
          "Introspect BeanInfo for bean[" + managedBean + "] failed, cause:\r\n" + e);
    }
    return opInfoList.toArray(new ModelMBeanOperationInfo[opInfoList.size()]);
  }
}
