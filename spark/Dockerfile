FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

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

USER spark