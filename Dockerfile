FROM eclipse-temurin:25-jre-alpine
COPY build/libs/dcre-prg-1.1.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
