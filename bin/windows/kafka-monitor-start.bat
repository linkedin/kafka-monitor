@echo off
REM Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
REM file except in compliance with the License. You may obtain a copy of the License at
REM
REM    http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
REM an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

setlocal enabledelayedexpansion

pushd %~dp0
set BASE_DIR=%CD%
popd


IF [%1] EQU [] (
	echo USAGE: %0 config/xinfra-monitor.properties
	EXIT /B 1
)

set COMMAND=%BASE_DIR%\kmf-run-class.bat com.linkedin.xinfra.monitor.XinfraMonitor %*

rem echo basedir: %BASE_DIR%

%COMMAND%

