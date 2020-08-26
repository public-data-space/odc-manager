FROM openjdk:12-alpine

RUN addgroup -S ids && adduser -S -g ids ids
WORKDIR /home/app/
RUN chown -R ids: ./ && chmod -R u+w ./
RUN mkdir -p /ids/repo/ && chown -R ids: /ids/repo/ && chmod -R u+w /ids/repo/
RUN mkdir -p /ids/certs/ && chown -R ids: /ids/certs/ && chmod -R u+w /ids/certs/
COPY /target/odc-manager-1.2.0-fat.jar .
EXPOSE 8080
USER ids
ENTRYPOINT ["java", "-jar", "./odc-manager-1.2.0-fat.jar"]
