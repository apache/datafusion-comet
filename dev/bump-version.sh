#!/usr/bin/env bash

set -eux -o pipefail

export PATH=$PATH:$JAVA_HOME/bin

# Figure out where the project root is
PROJECT_ROOT=$(git -C "$(dirname $0)" rev-parse --show-toplevel)

MVN="$PROJECT_ROOT/mvnw"

function exit_with_usage {
  set +x
  echo ""
  echo "bump-version.sh - tool for committing version increment"
  echo ""
  echo "usage:"
  cl_options="[--project-root <path>] [--mvn <mvn-command>] [--set-version <version>]"
  echo "bump-version.sh $cl_options"
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --project-root)
      PROJECT_ROOT=$(readlink -f "$2")
      shift
      ;;
    --mvn)
      MVN=$(readlink -f "$2")
      shift
      ;;
    --set-version)
      SET_VERSION="$2"
      shift
      ;;
    -h|--help)
      exit_with_usage
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done


if [ -z "$JAVA_HOME" ]; then
  # Fall back on JAVA_HOME from rpm, if found
  if command -v  rpm; then
    RPM_JAVA_HOME="$(rpm -E %java_home 2>/dev/null)"
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME="$RPM_JAVA_HOME"
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if ! command -v "$MVN" ; then
  echo -e "Could not locate Maven command: '$MVN'."
  echo -e "Specify the Maven command with the --mvn flag"
  exit -1
fi

pushd "$PROJECT_ROOT"

# Make sure the git directory is clean
git diff --exit-code
git diff --cached --exit-code

OLD_VERSION=$($MVN help:evaluate -Dexpression=project.version -q -DforceStdout)
OLD_RELEASE_VERSION=${OLD_VERSION%-SNAPSHOT}

# Increment the last digit of the version unless SET_VERSION is specified
# E.g. 3.2.0.66-apple-SNAPSHOT -> 3.2.0.67-apple-SNAPSHOT
# https://stackoverflow.com/a/21493080
# TODO: make sure perl is available if running this script in rio
NEW_VERSION="${SET_VERSION:-$(echo $OLD_VERSION | perl -pe 's/^((\d+\.)*)(\d+)(.*)$/$1.($3+1).$4/e')}"
NEW_RELEASE_VERSION=${NEW_VERSION%-SNAPSHOT}

$MVN versions:set -DnewVersion="$NEW_VERSION" -DgenerateBackupPoms=false | grep -v "no value"

sed -i '' 's/'"$OLD_RELEASE_VERSION"'/'"$NEW_RELEASE_VERSION"'/' rio.y*ml
# sed -i '' 's/'"$OLD_VERSION"'/'"$NEW_VERSION"'/' bin/comet-spark-shell

if [ -z "${SET_VERSION:-}" ]; then
  git commit -a -m "build: Bump to $NEW_VERSION after $OLD_RELEASE_VERSION release"
else
  git commit -a -m "build: Set version to $NEW_VERSION"
fi

set +x

# Update the lock filefile
cd native
cargo update

popd

if [ ! -z "$(git status -s native/Cargo.lock)" ]; then
  echo
  echo "Consider creating a pull request for the Cargo updates"
fi

echo
echo "SUCCESS! Please double check whether the generated commit has correct versions before pushing to the remote."
