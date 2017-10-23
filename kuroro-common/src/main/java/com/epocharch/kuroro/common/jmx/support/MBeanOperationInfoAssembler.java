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
import javax.management.Descriptor;
import javax.management.modelmbean.ModelMBeanOperationInfo;

public class MBeanOperationInfoAssembler {

  private static final String FIELD_CLASS = "class";
  private static final String FIELD_ROLE = "role";
  private static final String ROLE_OPERATION = "operation";
  private static final String ROLE_SETTER = "setter";
  private static final String ROLE_GETTER = "getter";
  private static final String FIELD_VISIBILITY = "visibility";
  // visibility : 1-4，其中 1 表示总是可见；4 表示几乎不可见
  private static final Integer ATTRIBUTE_VISIBILITY = new Integer(4);
  private final Object managedBean;
  private final Method method;

  public MBeanOperationInfoAssembler(Object managedBean, Method method) {
    this.managedBean = managedBean;
    this.method = method;
  }

  public ModelMBeanOperationInfo getMBeanOperationInfo() throws IntrospectionException {
    if (method.isSynthetic()) {
      return null;
    }
    if (method.getDeclaringClass().equals(Object.class)) {
      return null;
    }
    PropertyDescriptor propertyDescriptor = getPropDescriptor(method);
    if (propertyDescriptor != null) {
      if ((method.equals(propertyDescriptor.getReadMethod()) || method
          .equals(propertyDescriptor.getWriteMethod()))) {
        ModelMBeanOperationInfo operationInfo = new ModelMBeanOperationInfo(method.getName(),
            method);
        Descriptor desc = operationInfo.getDescriptor();
        if (method.equals(propertyDescriptor.getReadMethod())) {
          desc.setField(FIELD_ROLE, ROLE_GETTER);
        } else {
          desc.setField(FIELD_ROLE, ROLE_SETTER);
        }
        desc.setField(FIELD_VISIBILITY, ATTRIBUTE_VISIBILITY);
        desc.setField(FIELD_CLASS, managedBean.getClass().getName());
        operationInfo.setDescriptor(desc);
        return operationInfo;
      }
    } else {
      ModelMBeanOperationInfo operationInfo = new ModelMBeanOperationInfo(method.getName(), method);
      Descriptor descriptor = operationInfo.getDescriptor();
      descriptor.setField(FIELD_ROLE, ROLE_OPERATION);
      descriptor.setField(FIELD_CLASS, managedBean.getClass().getName());
      operationInfo.setDescriptor(descriptor);
      return operationInfo;
    }
    return null;
  }

  private PropertyDescriptor getPropDescriptor(Method method) throws IntrospectionException {
    Class<?> declaringClass = method.getDeclaringClass();
    BeanInfo beanInfo = Introspector.getBeanInfo(declaringClass);
    PropertyDescriptor[] propDescs = beanInfo.getPropertyDescriptors();
    for (PropertyDescriptor propDesc : propDescs) {
      if (method.equals(propDesc.getReadMethod()) || method.equals(propDesc.getWriteMethod())) {
        return propDesc;
      }
    }
    return null;
  }
}
