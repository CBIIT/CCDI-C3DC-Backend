# Build stage
FROM maven:3.6.3-openjdk-11 as build
    
WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage
FROM tomcat:9.0.79-jdk11 
#FROM cbiitssrepo/bento-backend:release

# install dependencies and clean up unused files
RUN apt-get update && apt-get install unzip
RUN rm -rf /usr/local/tomcat/webapps.dist
RUN rm -rf /usr/local/tomcat/webapps/ROOT

# expose ports
EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war
