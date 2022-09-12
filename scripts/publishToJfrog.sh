#!/usr/bin/env bash

result=${PWD##*/}
if [[ "$result" = "scripts" ]]
then
    echo "script must be run from root project folder, not $PWD"
    exit 1
else
    echo "we are in $PWD and tag is $RELEASE_TAG"

    if [[ $RELEASE_TAG =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
    then
        echo "publishing: tag $RELEASE_TAG looks like a semver"
        git status
        git describe --tags
        ./gradlew printVersion
        ./gradlew publishMyPublicationPublicationToLinkedInJfrogRepository
    else
        echo "not publishing: tag $RELEASE_TAG is NOT a valid semantic version (x.y.z)"
    fi
fi
