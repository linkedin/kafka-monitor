/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.common;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Path("/config")
public class ConfigPublisher {

  private final Map<String, ?> _values;

  public ConfigPublisher(Map<String, ?> values) {
    _values = values;
  }

  @GET
  @Path("/value/map")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, ?> valueMap() {
    return _values;
  }

}
