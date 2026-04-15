#!/bin/bash
#
SNAPSHOT=$1
echo Bumping versions up to $SNAPSHOT
./mvnw versions:set -DnewVersion=$SNAPSHOT -DgenerateBackupPoms=false
