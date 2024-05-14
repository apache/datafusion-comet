# Comet Release Process

This documentation is for creating an official source release of Apache DataFusion Comet. It does not currently 
cover creating binary releases.

The release process is based on the parent Apache DataFusion project, so please refer to the 
[DataFusion Release Process](https://github.com/apache/datafusion/blob/main/dev/release/README.md) for detailed 
instructions if you are not familiar with the release process here.

Here is a brief overview of the steps involved in creating a release:

- Create and merge a PR to update the version & update the changelog
- Tag the release with a release candidate tag e.g. 0.1.0-rc1
- Run the create-tarball script to create the source tarball and upload it to the dev site
- Start an email voting thread
- Once the vote passes, run the release-tarball script to move the tarball to the release site

## Verifying Release Candidates

The vote email will link to this section of this document, so this is where we will need to provide instructions for
verifying a release candidate.

The `dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification 
process. It checks the hashes and runs the tests.

```shell
./dev/release/verify-release-candidate.sh 0.1.0 1
```