# Verifying DataFusion Comet Release Candidates

The `dev/release/verify-release-candidate.sh` script in this repository can assist in the verification
process. It checks the hashes and runs the build. It does not run the test suite because this takes a long time
for this project and the test suites already run in CI before we create the release candidate, so running them
again is somewhat redundant.

```shell
./dev/release/verify-release-candidate.sh 0.1.0 1
```

We hope that users will verify the release beyond running this script by testing the release candidate with their
existing Spark jobs and report any functional issues or performance regressions.

Another way of verifying the release is to follow the
[Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html) and compare
performance with the previous release.
