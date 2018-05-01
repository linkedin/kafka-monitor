FROM debian:jessie

RUN apt-get -y update && \
    apt-get -y install netcat-traditional && \
    rm -rf /var/lib/apt/lists/*

COPY . /data/kafka-monitor
WORKDIR /data/kafka-monitor

EXPOSE 8080
ENTRYPOINT ["bash", "-c", "while true ; do  echo -e \"HTTP/1.1 200 OK\nContent-Type: text/plain; charset=utf-8\n\nWelcome to Moda! Your app (kafka-monitor)is set up correctly 🍦\" | nc -l -p 8080 -q 1 ; done"]
