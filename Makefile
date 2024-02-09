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

all: core jvm

core:
	cd core && cargo build
jvm:
	mvn clean package -DskipTests $(PROFILES)
test:
	mvn clean
	# We need to compile CometException so that the cargo test can pass
	mvn compile -pl common -DskipTests $(PROFILES)
	cd core && cargo build && \
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+${LD_LIBRARY_PATH}:}${JAVA_HOME}/lib:${JAVA_HOME}/lib/server:${JAVA_HOME}/lib/jli && \
	DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH:+${DYLD_LIBRARY_PATH}:}${JAVA_HOME}/lib:${JAVA_HOME}/lib/server:${JAVA_HOME}/lib/jli \
	RUST_BACKTRACE=1 cargo test
	SPARK_HOME=`pwd` COMET_CONF_DIR=$(shell pwd)/conf RUST_BACKTRACE=1 mvn verify $(PROFILES)
clean:
	cd core && cargo clean
	mvn clean
	rm -rf .dist
bench:
	cd core && LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+${LD_LIBRARY_PATH}:}${JAVA_HOME}/lib:${JAVA_HOME}/lib/server:${JAVA_HOME}/lib/jli && \
	DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH:+${DYLD_LIBRARY_PATH}:}${JAVA_HOME}/lib:${JAVA_HOME}/lib/server:${JAVA_HOME}/lib/jli \
	RUSTFLAGS="-Ctarget-cpu=native" cargo bench $(filter-out $@,$(MAKECMDGOALS))
format:
	mvn compile test-compile scalafix:scalafix -Psemanticdb $(PROFILES)
	mvn spotless:apply $(PROFILES)

core-amd64:
	rustup target add x86_64-apple-darwin
	cd core && RUSTFLAGS="-Ctarget-cpu=skylake -Ctarget-feature=-prefer-256-bit" CC=o64-clang CXX=o64-clang++ cargo build --target x86_64-apple-darwin --features nightly --release
	mkdir -p common/target/classes/org/apache/comet/darwin/x86_64
	cp core/target/x86_64-apple-darwin/release/libcomet.dylib common/target/classes/org/apache/comet/darwin/x86_64
	cd core && RUSTFLAGS="-Ctarget-cpu=haswell -Ctarget-feature=-prefer-256-bit" cargo build --features nightly --release
	mkdir -p common/target/classes/org/apache/comet/linux/amd64
	cp core/target/release/libcomet.so common/target/classes/org/apache/comet/linux/amd64
	jar -cf common/target/comet-native-x86_64.jar \
		-C common/target/classes/org/apache/comet darwin \
		-C common/target/classes/org/apache/comet linux
	./dev/deploy-file common/target/comet-native-x86_64.jar comet-native-x86_64${COMET_CLASSIFIER} jar

core-arm64:
	rustup target add aarch64-apple-darwin
	cd core && RUSTFLAGS="-Ctarget-cpu=apple-m1" CC=arm64-apple-darwin21.4-clang CXX=arm64-apple-darwin21.4-clang++ CARGO_FEATURE_NEON=1 cargo build --target aarch64-apple-darwin --features nightly --release
	mkdir -p common/target/classes/org/apache/comet/darwin/aarch64
	cp core/target/aarch64-apple-darwin/release/libcomet.dylib common/target/classes/org/apache/comet/darwin/aarch64
	cd core && RUSTFLAGS="-Ctarget-cpu=native" cargo build --features nightly --release
	mkdir -p common/target/classes/org/apache/comet/linux/aarch64
	cp core/target/release/libcomet.so common/target/classes/org/apache/comet/linux/aarch64
	jar -cf common/target/comet-native-aarch64.jar \
		-C common/target/classes/org/apache/comet darwin \
		-C common/target/classes/org/apache/comet linux
	./dev/deploy-file common/target/comet-native-aarch64.jar comet-native-aarch64${COMET_CLASSIFIER} jar

release-linux: clean
	rustup target add aarch64-apple-darwin x86_64-apple-darwin
	cd core && RUSTFLAGS="-Ctarget-cpu=apple-m1" CC=arm64-apple-darwin21.4-clang CXX=arm64-apple-darwin21.4-clang++ CARGO_FEATURE_NEON=1 cargo build --target aarch64-apple-darwin --features nightly --release
	cd core && RUSTFLAGS="-Ctarget-cpu=skylake -Ctarget-feature=-prefer-256-bit" CC=o64-clang CXX=o64-clang++ cargo build --target x86_64-apple-darwin --features nightly --release
	cd core && RUSTFLAGS="-Ctarget-cpu=native -Ctarget-feature=-prefer-256-bit" cargo build --features nightly --release
	mvn install -Prelease -DskipTests $(PROFILES)
release:
	cd core && RUSTFLAGS="-Ctarget-cpu=native" cargo build --features nightly --release
	mvn install -Prelease -DskipTests $(PROFILES)
benchmark-%: clean release
	cd spark && COMET_CONF_DIR=$(shell pwd)/conf MAVEN_OPTS='-Xmx20g' .mvn exec:java -Dexec.mainClass="$*" -Dexec.classpathScope="test" -Dexec.cleanupDaemonThreads="false" -Dexec.args="$(filter-out $@,$(MAKECMDGOALS))" $(PROFILES)
.DEFAULT:
	@: # ignore arguments provided to benchmarks e.g. "make benchmark-foo -- --bar", we do not want to treat "--bar" as target
