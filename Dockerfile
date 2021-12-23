FROM ftidataprodacr.azurecr.io/millitrans/connect-base

RUN curl -LJO https://wonderetldev.blob.core.windows.net/connect-lib/bc-fips-1.0.2.1.jar
RUN curl -LJO https://wonderetldev.blob.core.windows.net/connect-lib/bcpkix-fips-1.0.3.jar

RUN mkdir -p /usr/share/wonder/ct-connect
RUN cp bc-fips-1.0.2.1.jar /usr/share/wonder/ct-connect
RUN cp bcpkix-fips-1.0.3.jar /usr/share/wonder/ct-connect
COPY target/*.jar /usr/share/wonder/ct-connect/
