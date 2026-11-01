FROM eclipse-temurin:21-jre
WORKDIR /opt/activitytrail
COPY build/libs/activitytrail.jar activitytrail.jar
COPY db db
ENTRYPOINT ["java", "-jar", "/opt/activitytrail/activitytrail.jar"]
