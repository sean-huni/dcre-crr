FROM eclipse-temurin:25-jre-alpine
COPY build/libs/dcre-crr-0.1.0.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
