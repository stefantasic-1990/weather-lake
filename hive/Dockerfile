FROM apache/hive:3.1.3

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/* \
 && curl -sSL -o /opt/hive/lib/hadoop-aws-3.1.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.2/hadoop-aws-3.1.2.jar \
 && curl -sSL -o /opt/hive/lib/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

USER hive