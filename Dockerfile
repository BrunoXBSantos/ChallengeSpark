FROM bde2020/spark-base:3.1.1-hadoop3.2
COPY target/ChallengeSpark-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "challenge.App", "--master", "spark://spark-master:7077", "/ChallengeSpark-1.0-SNAPSHOT.jar"]
