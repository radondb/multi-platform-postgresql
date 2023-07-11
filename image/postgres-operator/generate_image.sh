#!/usr/bin/env bash
set -Eeo pipefail

source ../common/common.sh
build_image()
{
	image=$1
	platform=$2

	# image check
	pre_build_image "$image" && error=false || error=true
	if [ "$error" = "true" ]; then
		return
	fi

	echo "copy code ..."
	cp -r ../../platforms/kubernetes/postgres-operator/postgres .

	echo "build docker image $image ..."
	echo "notice: build all image need docker login. "

	build_cmd=$(get_build_cmd "$image" "$platform")
	$build_cmd
}

image=$(jq -r '.image' versions.json)
if [ "$platform" = arm64 ]; then
	image=${image}-arm64
fi

build_image $image "${platform}"
