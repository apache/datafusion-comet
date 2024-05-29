FROM centos:7
RUN ulimit -n 65536

# install common tools
#RUN yum update -y
RUN yum install -y centos-release-scl epel-release
RUN rm -f /var/lib/rpm/__db* && rpm --rebuilddb 
RUN yum clean all && rm -rf /var/cache/yum
RUN yum makecache
RUN yum install -y -v git
RUN yum install -y -v libzip unzip wget cmake3 openssl-devel
# install protoc
RUN wget -O /protobuf-21.7-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-x86_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-x86_64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc


# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-2023-08-01-x86_64-unknown-linux-gnu

RUN echo 'source $HOME/.cargo/env' >> ~/.bashrc

# install java
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> ~/.bashrc

# install maven
RUN yum install -y rh-maven35
RUN echo 'source /opt/rh/rh-maven35/enable' >> ~/.bashrc
RUN yum -y install gcc automake autoconf libtool make
