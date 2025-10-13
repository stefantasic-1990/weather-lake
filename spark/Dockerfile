FROM spark:4.0.0-scala2.13-java21-python3-ubuntu

USER root
RUN apt-get update && \
    apt-get install -y openssh-server netcat-openbsd && \
    rm -rf /var/lib/apt/lists/* && \
    \
    mkdir /var/run/sshd && \
    echo 'spark:spark' | chpasswd && \
    echo "PermitRootLogin no" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    \
    # Ensure started SSH sessions know the Java environment
    echo 'export JAVA_HOME=/opt/java/openjdk' >> /etc/profile && \
    echo 'export JAVA_VERSION=jdk-17.0.13+11' >> /etc/profile && \
    echo 'export PATH=/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' >> /etc/profile

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

USER spark