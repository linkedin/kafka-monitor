#!/bin/bash

set -x

#  wait for DNS services to be available
sleep 10

bin/kafka-monitor-start.sh config/kafka-monitor.properties
