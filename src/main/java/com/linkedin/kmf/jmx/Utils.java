/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * JMX utilities.
 */
public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static List<MbeanAttributeValue> getMBeanAttributeValues(String mbeanExpr, String attributeExpr) {
    List<MbeanAttributeValue> values = new ArrayList<>();
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      Set<ObjectName> mbeanNames = server.queryNames(new ObjectName(mbeanExpr), null);
      for (ObjectName mbeanName: mbeanNames) {
        MBeanInfo mBeanInfo = server.getMBeanInfo(mbeanName);
        MBeanAttributeInfo[] attributeInfos = mBeanInfo.getAttributes();
        for (MBeanAttributeInfo attributeInfo: attributeInfos) {
          if (attributeInfo.getName().equals(attributeExpr) || attributeExpr.length() == 0 || attributeExpr.equals("*")) {
            double value = (Double) server.getAttribute(mbeanName, attributeInfo.getName());
            values.add(new MbeanAttributeValue(mbeanName.getCanonicalName(), attributeInfo.getName(), value));
          }
        }
      }
    } catch (Exception e) {
      LOG.error("", e);
    }
    return values;
  }
}
