docker build -t comet_build_env_centos7:1.0 -f core/comet_build_env_centos7.dockerfile
export PROFILES="-Dmaven.test.skip=true -Pjdk1.8 -Pspark-3.5 -Dscalastyle.skip -Dspotless.check.skip=true -Denforcer.skip -Prelease-centos7 -Drat.skip=true"
cd core && RUSTFLAGS="-Ctarget-cpu=native" cross build --target x86_64-unknown-linux-gnu  --features nightly --release
cd ..
mvn install -Prelease -DskipTests ${PROFILES}