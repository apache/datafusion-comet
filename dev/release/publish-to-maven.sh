#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

###
# This file is based on the script release-build.sh in Spark
###

function usage {
  local NAME=$(basename $0)
  cat << EOF
usage: $NAME options

Publish signed artifacts to Maven.

Options
  -u ASF_USERNAME - Username of ASF committer account
  -r LOCAL_REPO - path to temporary local maven repo (created and written to by 'build-release-comet.sh')

The following will be prompted for -
  ASF_PASSWORD - Password of ASF committer account
  GPG_KEY - GPG key used to sign release artifacts
  GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
COMET_HOME_DIR=$SCRIPT_DIR/../..

ASF_USERNAME=""
ASF_PASSWORD=""

NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=789e15c00fd47

while getopts "u:r:h" opt; do
  case $opt in
    u) ASF_USERNAME="$OPTARG" ;;
    r) LOCAL_REPO="$OPTARG" ;;
    h) usage ;;
    \?) error "Invalid option. Run with -h for help." ;;
  esac
done

if [ "$LOCAL_REPO" == "" ]
then
  "Please provide the local Maven repository (from 'build-release-comet.sh')"
  usage
fi

if [ "$ASF_USERNAME" == "" ]
then
  read -p "ASF Username : " ASF_USERNAME && echo ""
fi
# Read some secret information
read -s -p "ASF Password : " ASF_PASSWORD && echo ""
read -s -p "GPG Key (Optional): " GPG_KEY && echo ""
read -s -p "GPG Passphrase : " GPG_PASSPHRASE && echo ""

if [ "$ASF_USERNAME" == "" ] || [ "$ASF_PASSWORD" == "" ] || [ "$GPG_PASSPHRASE" = "" ]
then
  echo "Missing credentials"
  exit 1
fi

# Default GPG command to use
GPG="gpg --pinentry-mode loopback"
if [ "$GPG_KEY" != "" ]
then
  GPG="$GPG -u $GPG_KEY"
fi

# sha1sum on linux, shasum on macos
SHA1SUM=$( which sha1sum || which shasum)

GIT_HASH=$(git rev-parse --short HEAD)

# REF: https://support.sonatype.com/hc/en-us/articles/213465818-How-can-I-programmatically-upload-an-artifact-into-Nexus-Repo-2
# REF: https://support.sonatype.com/hc/en-us/articles/213465868-Uploading-to-a-Nexus-Repository-2-Staging-Repository-via-REST-API
echo "Creating Nexus staging repository"

REPO_REQUEST="<promoteRequest><data><description>Apache Datafusion Comet $COMET_VERSION (commit $GIT_HASH)</description></data></promoteRequest>"
REPO_REQUEST_RESPONSE=$(curl -X POST -d "$REPO_REQUEST" -u $ASF_USERNAME:$ASF_PASSWORD \
  -H "Content-Type:application/xml"  \
  $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
if [ $? -ne 0 ]
then
  echo "Error creating staged repository"
  echo "$REPO_REQUEST_RESPONSE"
  exit 1
fi

STAGED_REPO_ID=$(echo $REPO_REQUEST_RESPONSE | xmllint --xpath "//stagedRepositoryId/text()" -)
echo "Created Nexus staging repository: $STAGED_REPO_ID"

if [ "$STAGED_REPO_ID" == "" ]
then
  echo "Error creating staged repository"
  echo "$REPO_REQUEST_RESPONSE"
  exit 1
fi

echo "Deploying artifacts from $LOCAL_REPO"

pushd $LOCAL_REPO/org/apache/datafusion

# Remove any extra files generated during install
find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

echo "Creating hash and signature files"
for file in $(find . -type f)
do
echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc \
  --detach-sig --armour $file;
if [ $(command -v md5) ]; then
  # Available on macOS; -q to keep only hash
  md5 -q $file > $file.md5
else
  # Available on Linux; cut to keep only hash
  md5sum $file | cut -f1 -d' ' > $file.md5
fi
$SHA1SUM $file | cut -f1 -d' ' > $file.sha1
done

NEXUS_UPLOAD=$NEXUS_ROOT/deployByRepositoryId/$STAGED_REPO_ID
echo "Uploading files to $NEXUS_UPLOAD"
for file in $(find . -type f)
do
  # strip leading ./
  FILE_SHORT=$(echo $file | sed -e "s/\.\///")
  DEST_URL="$NEXUS_UPLOAD/org/apache/datafusion/$FILE_SHORT"
  echo "  Uploading $FILE_SHORT"
  curl -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $FILE_SHORT $DEST_URL
  if [ $? -ne 0 ]
  then
    echo "    - Failed"
    exit 2
  fi
done

echo "Closing nexus staging repository"
REPO_REQUEST="<promoteRequest><data><stagedRepositoryId>$STAGED_REPO_ID</stagedRepositoryId><description>Apache Datafusion Comet $COMET_VERSION (commit $GIT_HASH)</description></data></promoteRequest>"
REPO_REQUEST_RESPONSE=$(curl -X POST -d "$REPO_REQUEST" -u $ASF_USERNAME:$ASF_PASSWORD \
  -H "Content-Type:application/xml" -v \
  $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
echo "Closed Nexus staging repository: $STAGED_REPO_ID"

popd