# Build stage
FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage
FROM tomcat:11.0.22-jdk21-temurin-noble AS fnl_base_image

RUN set -eux; \
    apt-get update; \
    apt-get -y full-upgrade; \
    apt-get install -y --no-install-recommends --only-upgrade \
        libc6 \
        libc-bin \
        locales \
        util-linux \
        util-linux-extra \
        mount \
        bsdutils \
        libblkid1 \
        libmount1 \
        libsmartcols1 \
        libuuid1; \
    apt-get install -y --no-install-recommends ca-certificates; \
    # Mitigate util-linux mount TOCTOU CVE path by disabling SUID binaries not needed in containers.
    if [ -f /usr/bin/mount ]; then chmod u-s /usr/bin/mount; fi; \
    if [ -f /usr/bin/umount ]; then chmod u-s /usr/bin/umount; fi; \
    # Remove toolchain packages if inherited from base layers; not needed at runtime.
    if dpkg-query -W -f='${Status}' binutils 2>/dev/null | grep -q "ok installed"; then \
        apt-get purge -y --auto-remove \
            binutils \
            binutils-aarch64-linux-gnu \
            binutils-common \
            libbinutils; \
    fi; \
    java -version; \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf /usr/local/tomcat/webapps.dist
RUN rm -rf /usr/local/tomcat/webapps/ROOT

# Modify the server.xml file to block error reportiing
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' conf/server.xml 

# expose ports
EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war
