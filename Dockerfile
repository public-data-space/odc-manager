FROM openjdk:12-alpine
RUN groupadd -r ids && useradd -r -g ids ids
WORKDIR /home/app/
RUN chown ids: ./ && chmod u+w ./
RUN mkdir -p /ids/repo/ && chown ids: /ids/repo/ && chmod u+w /ids/repo/
COPY /target/odc-manager-1.0-SNAPSHOT-fat.jar .
EXPOSE 8080
USER ids
ENTRYPOINT ["java","-jar","./odc-manager-1.0-SNAPSHOT-fat.jar"]
