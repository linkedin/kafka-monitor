FROM anapsix/alpine-java

WORKDIR /opt/kafka-monitor
ADD build/ build/
ADD bin/kafka-monitor-start.sh bin/kafka-monitor-start.sh
ADD bin/kmf-run-class.sh bin/kmf-run-class.sh
ADD config/kafka-monitor.properties config/env/production/kafka-monitor.properties
ADD config/log4j.properties config/log4j.properties
ADD docker/kafka-monitor-docker-entry.sh docker/kafka-monitor-docker-entry.sh
ADD webapp/ webapp/

CMD ["/opt/kafka-monitor/docker/kafka-monitor-docker-entry.sh"]