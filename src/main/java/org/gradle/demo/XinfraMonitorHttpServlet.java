/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package org.gradle.demo;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


@WebServlet(name = "XinfraMonitorHttpServlet", urlPatterns = {"hello"}, loadOnStartup = 1)
public class XinfraMonitorHttpServlet extends HttpServlet {

  /**
   * @param request HTTP Servlet Request
   * @param response HTTP Servlet Response
   * @throws IOException Input Output Exception
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.getWriter().print("Hello, World!");
  }

  /**
   * POST method
   * @param request HTTP Servlet Request
   * @param response HTTP Servlet Response
   * @throws ServletException
   * @throws IOException
   */
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String name = request.getParameter("name");
    if (name == null) {
      name = "World";
    }
    request.setAttribute("user", name);
    request.getRequestDispatcher("response.jsp").forward(request, response);
  }
}

