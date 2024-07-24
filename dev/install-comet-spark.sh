#!/bin/sh
set -e

# Helper script to install Boson and Spark in Rio
SCRIPT_NAME=$(echo $0 | sed 's/.*\///')

function usage() {
  echo "Usage: $SCRIPT_NAME [--use-existing-comet <version>] [<spark-branch>]"
  echo "       $SCRIPT_NAME -h|--help"

}

function usageAndExit(){
  usage
  exit 0
}

# complain to STDERR and exit with error
die() {
  echo "$*" >&2;
  exit 2;
}

BASEDIR=$(readlink -f "$(dirname $0)")
COMET_VERSION=""
COMET_WORKSPACE=$(cd $BASEDIR/..; pwd)

SKIP_COMET=false
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      usageAndExit
      ;;
    --use-existing-comet)
      SKIP_COMET=true
      COMET_VERSION=$2
      shift 2
      ;;
    -*)
      die "Unknown option: $1"
      ;;
    *)
      break;
      ;;
  esac
done

if [ -z "$COMET_VERSION" ]; then
  COMET_VERSION=$($COMET_WORKSPACE/mvnw -nsu -q $PROFILES help:evaluate -Dexpression=project.version -DforceStdout 2>/dev/null)
fi
SPARK_VERSION=$(./mvnw -nsu -q $PROFILES help:evaluate -Dexpression=spark.version -DforceStdout)
SPARK_MINOR_VERSION=$(echo $SPARK_VERSION | sed -E 's/([0-9]+\.[0-9]+).*/\1/')
SPARK_PATCH_VERSION=$(echo $SPARK_VERSION | sed -E 's/([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
SPARK_BRANCH=${1:-"branch-${SPARK_PATCH_VERSION}-apple"}
SCALA_BINARY_VERSION=$($COMET_WORKSPACE/mvnw -nsu -q $PROFILES help:evaluate -Dexpression=scala.binary.version -DforceStdout 2>/dev/null)
SCALA_VERSION=$($COMET_WORKSPACE/mvnw -nsu -q $PROFILES help:evaluate -Dexpression=scala.version -DforceStdout 2>/dev/null)

if [ $SKIP_COMET = "true" ]; then
  echo Skipping comet build...

  # Force using the provided Comet version by purging previous snapshots from the local repository
  $COMET_WORKSPACE/mvnw dependency:purge-local-repository -DmanualInclude=org.apache.comet:comet-spark-spark${SPARK_MINOR_VERSION}_${SCALA_BINARY_VERSION}
else
  make clean release
fi

set -x

# Removing local cached parquet artifacts since they don't include test jars, which
# could cause issues later when building Spark with SBT
rm -rf /root/.m2/repository/org/apache/parquet
# Removing local cached protobuf that also causes building Spark with SBT
rm -rf /root/.m2/repository/com/google/protobuf

cd $COMET_WORKSPACE
rm -rf apache-spark
git clone git@github.pie.apple.com:IPR/apache-spark.git --depth 1 --branch $SPARK_BRANCH
cd apache-spark

# Apply custom diff files, if they exist
if [ -f "$BASEDIR/diffs/$SPARK_BRANCH.diff" ]; then
  git apply "$BASEDIR/diffs/$SPARK_BRANCH.diff"
fi

# Update the Comet version
$COMET_WORKSPACE/mvnw -nsu -q versions:set-property -Dproperty=comet.version  -DnewVersion=$COMET_VERSION -DgenerateBackupPoms=false

# Update the Scala version
dev/change-scala-version.sh $SCALA_BINARY_VERSION

# Store the Boson parameters
cat > comet-parameters.sh <<EOD
export COMET_WORKSPACE=$COMET_WORKSPACE
export COMET_VERSION=$COMET_VERSION
export SCALA_VERSION=$SCALA_VERSION
export SCALA_BINARY_VERSION=$SCALA_BINARY_VERSION
export SPARK_BRANCH=$SPARK_BRANCH
EOD
chmod a+x comet-parameters.sh
