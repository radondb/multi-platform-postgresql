#!/usr/bin/env bash
set -Eeo pipefail

build_image()
{
	image=$1
	platform=$2

	image_exists=$( docker image ls --format "{{.Repository}}:{{.Tag}}" | awk -v aaa=$image '{print $0} END{print aaa}' | grep -c $image )
	if [ "$image_exists" -ne 1 ]; then
		if [ "$forcebuildimage" = 1 ]; then
			echo "docker image $image exists, rebuilding the image ..."
		else
			echo "docker image $image exists, skiping ..."
			return
		fi
	fi

	echo "copy code ..."
	cp -r ../../platforms/kubernetes/postgres-operator/postgres .

	echo "build docker image $image ..."
	docker builder build --no-cache -t $image --platform $platform .
}

image=$(jq -r '.image' versions.json)
if [ "$platform" = arm64 ]; then
	image=${image}-arm64
fi

build_image $image "linux/${platform}"
