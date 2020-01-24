/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.common;

public class MbeanAttributeValue {
  private final String _mbean;
  private final String _attribute;
  private final double _value;

  public MbeanAttributeValue(String mbean, String attribute, double value) {
    _mbean = mbean;
    _attribute = attribute;
    _value = value;
  }

  public String mbean() {
    return _mbean;
  }

  public String attribute() {
    return _attribute;
  }

  public double value() {
    return _value;
  }

  @Override
  public String toString() {
    return _mbean + ":" + _attribute + "=" + _value;
  }
}
