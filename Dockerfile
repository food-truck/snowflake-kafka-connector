FROM confluentinc/cp-kafka-connect-base

USER root

RUN sed -i 's/^plugin.path=/plugin.path=\/usr\/share\/wonder,/' /etc/kafka/connect-distributed.properties \
    && sed -i 's/^plugin.path=/plugin.path=\/usr\/share\/wonder,/' /etc/kafka/connect-standalone.properties \
    && sed -i 's/^plugin.path=/plugin.path=\/usr\/share\/wonder,/' /etc/schema-registry/connect-avro-distributed.properties \
    && sed -i 's/^plugin.path=/plugin.path=\/usr\/share\/wonder,/' /etc/schema-registry/connect-avro-standalone.properties

RUN mkdir "/usr/share/wonder"

#RUN curl -LJO https://wonderetldev.blob.core.windows.net/connect-lib/bc-fips-1.0.2.1.jar
#RUN curl -LJO https://wonderetldev.blob.core.windows.net/connect-lib/bcpkix-fips-1.0.3.jar

RUN mkdir -p /usr/share/wonder/ct-connect
#RUN cp bc-fips-1.0.2.1.jar /usr/share/wonder/ct-connect
#RUN cp bcpkix-fips-1.0.3.jar /usr/share/wonder/ct-connect
RUN curl -o /usr/share/java/confluent-common/common-logging-7.0.1.jar -SL https://packages.confluent.io/maven/io/confluent/common-logging/7.0.1/common-logging-7.0.1.jar
RUN curl -o /usr/share/java/confluent-common/confluent-log4j-extensions-7.0.1.jar -SL https://packages.confluent.io/maven/io/confluent/confluent-log4j-extensions/7.0.1/confluent-log4j-extensions-7.0.1.jar

COPY connect-log4j.properties /etc/confluent/docker/log4j.properties.template

COPY target/*.jar /usr/share/wonder/ct-connect/
