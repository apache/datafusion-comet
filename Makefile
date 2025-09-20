# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

.PHONY: all core jvm test clean release-linux release bench

define spark_jvm_17_extra_args
$(shell ./mvnw help:evaluate -Dexpression=extraJavaTestArgs | grep -v '\[')
endef

# Build optional Comet native features (like hdfs e.g)
FEATURES_ARG := $(shell ! [ -z $(COMET_FEATURES) ] && echo '--features=$(COMET_FEATURES)')

all: core jvm

core:
	cd native && cargo build $(FEATURES_ARG)
test-rust:
	# We need to compile CometException so that the cargo test can pass
	./mvnw compile -pl common -DskipTests $(PROFILES)
	cd native && cargo build $(FEATURES_ARG) && \
	RUST_BACKTRACE=1 cargo test $(FEATURES_ARG)
jvm:
	./mvnw clean package -DskipTests $(PROFILES)
test-jvm: core
	SPARK_HOME=`pwd` COMET_CONF_DIR=$(shell pwd)/conf RUST_BACKTRACE=1 ./mvnw verify $(PROFILES)
test: test-rust test-jvm
clean:
	cd native && cargo clean
	./mvnw clean $(PROFILES)
	rm -rf .dist
bench:
	cd native && RUSTFLAGS="-Ctarget-cpu=native" cargo bench $(FEATURES_ARG) $(filter-out $@,$(MAKECMDGOALS))
format:
	cd native && cargo fmt
	./mvnw compile test-compile scalafix:scalafix -Psemanticdb $(PROFILES)
	./mvnw spotless:apply $(PROFILES)

# build native libs for amd64 architecture Linux/MacOS on a Linux/amd64 machine/container
core-amd64-libs:
	cd native && cargo build -j 2 --release $(FEATURES_ARG)
ifdef HAS_OSXCROSS
	rustup target add x86_64-apple-darwin
	cd native && cargo build -j 2 --target x86_64-apple-darwin --release $(FEATURES_ARG)
endif

# build native libs for arm64 architecture Linux/MacOS on a Linux/arm64 machine/container
core-arm64-libs:
	cd native && cargo build -j 2 --release $(FEATURES_ARG)
ifdef HAS_OSXCROSS
	rustup target add aarch64-apple-darwin
	cd native && cargo build -j 2 --target aarch64-apple-darwin --release $(FEATURES_ARG)
endif

core-amd64:
	rustup target add x86_64-apple-darwin
	cd native && RUSTFLAGS="-Ctarget-cpu=skylake -Ctarget-feature=-prefer-256-bit" CC=o64-clang CXX=o64-clang++ cargo build --target x86_64-apple-darwin --release $(FEATURES_ARG)
	mkdir -p common/target/classes/org/apache/comet/darwin/x86_64
	cp native/target/x86_64-apple-darwin/release/libcomet.dylib common/target/classes/org/apache/comet/darwin/x86_64
	cd native && RUSTFLAGS="-Ctarget-cpu=haswell -Ctarget-feature=-prefer-256-bit" cargo build --release $(FEATURES_ARG)
	mkdir -p common/target/classes/org/apache/comet/linux/amd64
	cp native/target/release/libcomet.so common/target/classes/org/apache/comet/linux/amd64
	jar -cf common/target/comet-native-x86_64.jar \
		-C common/target/classes/org/apache/comet darwin \
		-C common/target/classes/org/apache/comet linux
	./dev/deploy-file common/target/comet-native-x86_64.jar comet-native-x86_64${COMET_CLASSIFIER} jar

core-arm64:
	rustup target add aarch64-apple-darwin
	cd native && RUSTFLAGS="-Ctarget-cpu=apple-m1" CC=arm64-apple-darwin21.4-clang CXX=arm64-apple-darwin21.4-clang++ CARGO_FEATURE_NEON=1 cargo build --target aarch64-apple-darwin --release $(FEATURES_ARG)
	mkdir -p common/target/classes/org/apache/comet/darwin/aarch64
	cp native/target/aarch64-apple-darwin/release/libcomet.dylib common/target/classes/org/apache/comet/darwin/aarch64
	cd native && RUSTFLAGS="-Ctarget-cpu=native" cargo build --release $(FEATURES_ARG)
	mkdir -p common/target/classes/org/apache/comet/linux/aarch64
	cp native/target/release/libcomet.so common/target/classes/org/apache/comet/linux/aarch64
	jar -cf common/target/comet-native-aarch64.jar \
		-C common/target/classes/org/apache/comet darwin \
		-C common/target/classes/org/apache/comet linux
	./dev/deploy-file common/target/comet-native-aarch64.jar comet-native-aarch64${COMET_CLASSIFIER} jar

release-linux: clean
	rustup target add aarch64-apple-darwin x86_64-apple-darwin
	cd native && RUSTFLAGS="-Ctarget-cpu=apple-m1" CC=arm64-apple-darwin21.4-clang CXX=arm64-apple-darwin21.4-clang++ CARGO_FEATURE_NEON=1 cargo build --target aarch64-apple-darwin --release $(FEATURES_ARG)
	cd native && RUSTFLAGS="-Ctarget-cpu=skylake -Ctarget-feature=-prefer-256-bit" CC=o64-clang CXX=o64-clang++ cargo build --target x86_64-apple-darwin --release $(FEATURES_ARG)
	cd native && RUSTFLAGS="-Ctarget-cpu=native -Ctarget-feature=-prefer-256-bit" cargo build --release $(FEATURES_ARG)
	./mvnw install -Prelease -DskipTests $(PROFILES)
release:
	cd native && RUSTFLAGS="$(RUSTFLAGS) -Ctarget-cpu=native" cargo build --release $(FEATURES_ARG)
	./mvnw install -Prelease -DskipTests $(PROFILES)
release-nogit:
	cd native && RUSTFLAGS="-Ctarget-cpu=native" cargo build --release
	./mvnw install -Prelease -DskipTests $(PROFILES) -Dmaven.gitcommitid.skip=true
benchmark-%: release
	cd spark && COMET_CONF_DIR=$(shell pwd)/conf MAVEN_OPTS='-Xmx20g ${call spark_jvm_17_extra_args}' ../mvnw exec:java -Dexec.mainClass="$*" -Dexec.classpathScope="test" -Dexec.cleanupDaemonThreads="false" -Dexec.args="$(filter-out $@,$(MAKECMDGOALS))" $(PROFILES)
.DEFAULT:
	@: # ignore arguments provided to benchmarks e.g. "make benchmark-foo -- --bar", we do not want to treat "--bar" as target
