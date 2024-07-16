FROM apache/spark:3.4.2

USER root

# Installing JDK11 as the image comes with JRE
RUN apt update \
    && apt install -y git \
    && apt install -y curl \
    && apt install -y openjdk-11-jdk \
    && apt clean

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
ENV RUSTFLAGS="-C debuginfo=line-tables-only -C incremental=false"
ENV SPARK_VERSION=3.4
ENV SCALA_VERSION=2.12

# Pick the JDK instead of JRE to compile Comet
RUN cd /opt \
    && git clone https://github.com/apache/datafusion-comet.git \
    && cd datafusion-comet \
    && JAVA_HOME=$(readlink -f $(which javac) | sed "s/\/bin\/javac//") make release PROFILES="-Pspark-$SPARK_VERSION -Pscala-$SCALA_VERSION"

RUN cp /opt/datafusion-comet/spark/target/comet-spark-spark${SPARK_VERSION}_$SCALA_VERSION-0.1.0-SNAPSHOT.jar $SPARK_HOME/jars

USER ${spark_uid}