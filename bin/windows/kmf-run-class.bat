@echo off
REM Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
REM file except in compliance with the License. You may obtain a copy of the License at
REM
REM    http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
REM an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

setlocal enabledelayedexpansion

IF [%1] EQU [] (
	echo USAGE: %0 com.linkedin.kmf.KafkaMonitor config/kafka-monitor.properties
	EXIT /B 1
)

IF [%2] EQU [] (
	echo USAGE: %0 %1 config/kafka-monitor.properties
	EXIT /B 1
)


rem Using pushd popd to set BASE_DIR to the absolute path
pushd %~dp0..\..
set BASE_DIR=%CD%
popd

IF ["%SCALA_VERSION%"] EQU [""] (
  set SCALA_VERSION=2.10.6
)

IF ["%SCALA_BINARY_VERSION%"] EQU [""] (
  set SCALA_BINARY_VERSION=2.10
)

rem Classpath addition for kafka-core dependencies
for %%i in (%BASE_DIR%\build\libs\*.jar) do (
	call :concat %%i
)

rem Classpath addition for kafka-examples
for %%i in (%BASE_DIR%\build\dependant-libs\*.jar) do (
	call :concat %%i
)

rem JMX settings
IF ["%KAFKA_JMX_OPTS%"] EQU [""] (
	set KAFKA_JMX_OPTS=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false
)

rem JMX port to use
IF ["%JMX_PORT%"] NEQ [""] (
	set KAFKA_JMX_OPTS=%KAFKA_JMX_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
)

rem Log directory to use
IF ["%LOG_DIR%"] EQU [""] (
    set LOG_DIR=%BASE_DIR%\logs
)

rem Log4j settings
IF ["%KAFKA_LOG4J_OPTS%"] EQU [""] (
	set KAFKA_LOG4J_OPTS=-Dlog4j.configurationFile=%BASE_DIR%\config\log4j2.properties
) ELSE (
  # create logs directory
  IF not exist %LOG_DIR% (
      mkdir %LOG_DIR%
  )
)

set KAFKA_LOG4J_OPTS=-Dkafka.logs.dir=%LOG_DIR% %KAFKA_LOG4J_OPTS%

rem Generic jvm settings you want to add
IF ["%KAFKA_OPTS%"] EQU [""] (
	set KAFKA_OPTS=
)

set DEFAULT_JAVA_DEBUG_PORT=5005
set DEFAULT_DEBUG_SUSPEND_FLAG=n
rem Set Debug options if enabled
IF ["%KAFKA_DEBUG%"] NEQ [""] (


	IF ["%JAVA_DEBUG_PORT%"] EQU [""] (
		set JAVA_DEBUG_PORT=%DEFAULT_JAVA_DEBUG_PORT%
	)

	IF ["%DEBUG_SUSPEND_FLAG%"] EQU [""] (
		set DEBUG_SUSPEND_FLAG=%DEFAULT_DEBUG_SUSPEND_FLAG%
	)
	set DEFAULT_JAVA_DEBUG_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=!DEBUG_SUSPEND_FLAG!,address=!JAVA_DEBUG_PORT!

	IF ["%JAVA_DEBUG_OPTS%"] EQU [""] (
		set JAVA_DEBUG_OPTS=!DEFAULT_JAVA_DEBUG_OPTS!
	)

	echo Enabling Java debug options: !JAVA_DEBUG_OPTS!
	set KAFKA_OPTS=!JAVA_DEBUG_OPTS! !KAFKA_OPTS!
)

rem Which java to use
IF ["%JAVA_HOME%"] EQU [""] (
	set JAVA=java
) ELSE (
	set JAVA="%JAVA_HOME%\bin\java"
)

rem Memory options
IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
	set KAFKA_HEAP_OPTS=-Xmx256M
)

rem JVM performance options
IF ["%KAFKA_JVM_PERFORMANCE_OPTS%"] EQU [""] (
	set KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true
)

IF ["%CLASSPATH%"] EQU [""] (
	echo Classpath is empty. Please build the project first e.g. by running 'gradlew jarAll'
	EXIT /B 2
)

set COMMAND=%JAVA% %KAFKA_HEAP_OPTS% %KAFKA_JVM_PERFORMANCE_OPTS% %KAFKA_JMX_OPTS% %KAFKA_LOG4J_OPTS% -cp %CLASSPATH% %KAFKA_OPTS% %*
rem echo.
echo %COMMAND%
rem echo.

%COMMAND%

goto :eof
:concat
IF ["%CLASSPATH%"] EQU [""] (
  set CLASSPATH="%1"
) ELSE (
  set CLASSPATH=%CLASSPATH%;"%1"
)


REM while [ $# -gt 0 ]; do
REM   COMMAND=$1
REM   case $COMMAND in
REM     -name)
REM       DAEMON_NAME=$2
REM       CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
REM       shift 2
REM       ;;
REM     -loggc)
REM       if [ -z "$KAFKA_GC_LOG_OPTS" ]; then
REM         GC_LOG_ENABLED="true"
REM       fi
REM       shift
REM       ;;
REM     -daemon)
REM       DAEMON_MODE="true"
REM       shift
REM       ;;
REM     *)
REM       break
REM       ;;
REM   esac
REM done

REM GC options
REM GC_FILE_SUFFIX='-gc.log'
REM GC_LOG_FILE_NAME=''
REM if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
REM   GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
REM   KAFKA_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails REM -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
REM fi

REM Launch mode
REM if [ "x$DAEMON_MODE" = "xtrue" ]; then
REM   nohup $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS -cp $CLASSPATH REM $KAFKA_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &REM
REM elseREM
REM   exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS REM $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@"
REM fi
