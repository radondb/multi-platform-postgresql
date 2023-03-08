#!/usr/bin/env bash
set -Eeo pipefail

source ../common/common.sh
build_image()
{
	image=$1
	platform=$2

	# image check
	pre_build_image "$image" || error=true
	if [ "$error" ]; then
		return
	fi

	echo "build docker image $image ..."
	echo "notice: build all image need docker login. "

	build_cmd=$(get_build_cmd "$image" "$platform")
	$build_cmd
}

image=$(jq -r '.image' versions.json)
if [ "$platform" = arm64 ]; then
	image=${image}-arm64
fi


cat queries.yaml autofailover.yaml > autofailover_queries.yaml

# get queries.yaml
# wget https://raw.githubusercontent.com/prometheus-community/postgres_exporter/master/queries.yaml

build_image $image "${platform}"
