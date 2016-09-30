FROM anapsix/alpine-java

MAINTAINER coffeepac@gmail.com

WORKDIR /opt/kafka-monitor
ADD build/ build/
ADD core/build/ core/build/
ADD services/build/ services/build/
ADD tests/build/ tests/build/
ADD bin/kafka-monitor-start.sh bin/kafka-monitor-start.sh
ADD bin/kmf-run-class.sh bin/kmf-run-class.sh
ADD config/kafka-monitor.properties config/kafka-monitor.properties
ADD config/log4j.properties config/log4j.properties
ADD docker/kafka-monitor-docker-entry.sh kafka-monitor-docker-entry.sh
ADD webapp/ webapp/

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["./kafka-monitor-docker-entry.sh"]
