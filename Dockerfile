# Build stage
FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /usr/src/app
COPY . .
RUN mvn package -DskipTests

# Production stage
FROM tomcat:11.0.22-jdk21-temurin-jammy AS fnl_base_image

# Upgrade OS packages, install deps, and update Java to Temurin 21.0.10.
ARG TEMURIN_DIR=jdk-21.0.10+7
ARG TEMURIN_BUILD=21.0.10_7
# Official SHA256 checksums for the Temurin JDK tarballs for the above TEMURIN_DIR/TEMURIN_BUILD.
ARG TEMURIN_SHA256_AMD64="ea3b9bd464d6dd253e9a7accf59f7ccd2a36e4aa69640b7251e3370caef896a4"
ARG TEMURIN_SHA256_ARM64="357fee29fb0d5c079f6730db98b28942df13a6eed426f6c61cd4ad703ab27b9a"
RUN set -eux; \
    apt-get update; \
    apt-get -y upgrade; \
    apt-get install -y --no-install-recommends ca-certificates curl tar unzip; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) temurin_arch="x64"; temurin_sha256="$TEMURIN_SHA256_AMD64" ;; \
      arm64) temurin_arch="aarch64"; temurin_sha256="$TEMURIN_SHA256_ARM64" ;; \
      *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.10%2B7/OpenJDK21U-jdk_${temurin_arch}_linux_hotspot_${TEMURIN_BUILD}.tar.gz"; \
    mkdir -p /opt/java; \
    curl -fsSL "$url" -o /tmp/temurin.tgz; \
    echo "$temurin_sha256  /tmp/temurin.tgz" | sha256sum -c -; \
    tar -xzf /tmp/temurin.tgz -C /opt/java; \
    rm /tmp/temurin.tgz; \
    rm -rf /opt/java/openjdk; \
    mv "/opt/java/${TEMURIN_DIR}" /opt/java/openjdk; \
    java -version; \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf /usr/local/tomcat/webapps.dist
RUN rm -rf /usr/local/tomcat/webapps/ROOT

# Modify the server.xml file to block error reportiing
RUN sed -i 's|</Host>|  <Valve className="org.apache.catalina.valves.ErrorReportValve"\n               showReport="false"\n               showServerInfo="false" />\n\n      </Host>|' conf/server.xml 

# expose ports
EXPOSE 8080

COPY --from=build /usr/src/app/target/Bento-0.0.1.war /usr/local/tomcat/webapps/ROOT.war
