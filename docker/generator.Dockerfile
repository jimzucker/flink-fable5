FROM eclipse-temurin:17-jre
COPY target/flink-demo.jar /opt/app/flink-demo.jar
ENTRYPOINT ["java", "-cp", "/opt/app/flink-demo.jar", "com.demo.flink.generator.DataGenerator"]
