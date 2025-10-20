FROM apache/spark:3.5.7-python3

USER root
RUN apt-get update && \
    apt-get install -y openssh-server netcat-openbsd && \
    rm -rf /var/lib/apt/lists/* && \

    mkdir /var/run/sshd && \
    echo 'spark:spark' | chpasswd && \
    echo "PermitRootLogin no" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \

    mkdir /artifacts && \
    chown -R 185:185 /artifacts && \
    chmod -R 755 /artifacts && \

    mkdir -p /opt/spark/logs && \
    chown -R spark:spark /opt/spark/logs && \
    chmod -R 755 /opt/spark/logs && \

    # Ensure started SSH sessions know the Java environment
    echo 'export JAVA_HOME=/opt/java/openjdk' >> /etc/profile && \
    echo 'export JAVA_VERSION=jdk-17.0.13+11' >> /etc/profile && \
    echo 'export PATH=/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' >> /etc/profile

RUN mkdir -p /opt/hive-jars
RUN curl -fsSL -o /opt/hive-jars/hive-common-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-common/3.1.3/hive-common-3.1.3.jar
RUN curl -fsSL -o /opt/hive-jars/hive-metastore-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar
RUN curl -fsSL -o /opt/hive-jars/hive-serde-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-serde/3.1.3/hive-serde-3.1.3.jar
RUN curl -fsSL -o /opt/hive-jars/hive-exec-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar
RUN curl -fsSL -o /opt/hive-jars/hive-storage-api-2.7.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-storage-api/2.7.3/hive-storage-api-2.7.3.jar
RUN curl -fsSL -o /opt/hive-jars/libfb303-0.9.3.jar https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar
RUN curl -fsSL -o /opt/hive-jars/libthrift-0.12.0.jar https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar
RUN curl -fsSL -o /opt/hive-jars/commons-logging-1.2.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar
RUN curl -fsSL -o /opt/hive-jars/guava-27.0-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar
RUN curl -fsSL -o /opt/hive-jars/commons-lang3-3.9.jar https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.9/commons-lang3-3.9.jar
RUN curl -fsSL -o /opt/hive-jars/slf4j-api-1.7.25.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar
RUN curl -fsSL -o /opt/hive-jars/commons-io-2.8.0.jar https://repo1.maven.org/maven2/commons-io/commons-io/2.8.0/commons-io-2.8.0.jar
RUN curl -fsSL -o /opt/hive-jars/calcite-core-1.18.0.jar https://repo1.maven.org/maven2/org/apache/calcite/calcite-core/1.18.0/calcite-core-1.18.0.jar
RUN curl -fsSL -o /opt/hive-jars/calcite-linq4j-1.18.0.jar https://repo1.maven.org/maven2/org/apache/calcite/calcite-linq4j/1.18.0/calcite-linq4j-1.18.0.jar
RUN curl -fsSL -o /opt/hive-jars/avatica-core-1.13.0.jar https://repo1.maven.org/maven2/org/apache/calcite/avatica/avatica-core/1.13.0/avatica-core-1.13.0.jar
RUN curl -fsSL -o /opt/hive-jars/javax.servlet-api-3.1.0.jar https://repo1.maven.org/maven2/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0.jar
RUN curl -fsSL -o /opt/hive-jars/hive-cli-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-cli/3.1.3/hive-cli-3.1.3.jar
RUN curl -fsSL -o /opt/hive-jars/hive-jdbc-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3.jar
    
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar

USER spark