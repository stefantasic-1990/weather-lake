FROM spark:4.0.0-scala2.12-java17-python3-ubuntu

USER root
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

ENV HADOOP_VERSION=3.3.6
ENV AWS_BUNDLE_VERSION=1.12.262

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
        -P /opt/spark/jars/ \
 && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar \
        -P /opt/spark/jars/

USER spark