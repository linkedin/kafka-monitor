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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generates the table of configuration parameters, their documentation strings and default values.
 */
public class ConfigDocumentationGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigDocumentationGenerator.class);

  private static void printHelp() {
    System.out.println("ConfigDocumentationGenerator outputDirectory configClassNames...");
  }

  private static void printHtmlHeader(Writer out, String docClass) throws IOException {
    out.write("<html><head><title>Kafka Monitoring Automatically Generated Documentation. </title></head><body>\n");
    out.write("<h1>");
    out.write(docClass);
    out.write("</h1>\n");
  }
  private static void printHtmlFooter(Writer out) throws IOException {
    out.write("</body>\n</html>\n");
  }

  public static void main(String[] argv) throws Exception {
    if (argv.length < 2) {
      printHelp();
      System.exit(1);
    }

    File outputDir = new File(argv[0]);
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    for (int i = 1; i < argv.length; i++) {
      Class<? extends AbstractConfig> configClass = (Class<? extends AbstractConfig>) Class.forName(argv[i]);
      Field configDefField = configClass.getDeclaredField("CONFIG");
      configDefField.setAccessible(true);
      ConfigDef configDef = (ConfigDef) configDefField.get(null);
      String docClass = configClass.getSimpleName();
      File outputFile = new File(outputDir, docClass + ".html");
      try (FileWriter fout = new FileWriter(outputFile)) {
        printHtmlHeader(fout, docClass);
        fout.write(configDef.toHtmlTable());
        printHtmlFooter(fout);
      }
    }
  }
}
