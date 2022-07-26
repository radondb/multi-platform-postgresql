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

	echo "build docker image $image ..."
	build_cmd="docker"
	docker_version=$(docker version --format '{{index (split .Server.Version ".") 0}}')

	if [ $docker_version -ge 20 ]; then
		build_cmd="$build_cmd buildx build --no-cache"
	else
		build_cmd="$build_cmd builder build --no-cache"
	fi

	if [ $platform == "all" ]; then
		echo "build all image need docker login. "
		builder_exists=$(docker buildx ls | awk '{if ($1=="multi-platform") print $1}')
		if [ "$builder_exists" ]; then
			docker buildx rm multi-platform
		fi
		# create a new builder instance
		docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64

		temp=($(echo $image | tr "/" " "))
		if [ ${#temp[@]} == 1 ]; then
			image="$namespace/$image"
		fi

		build_cmd="$build_cmd --push --platform linux/amd64,linux/arm64"
	else
		build_cmd="$build_cmd --platform linux/${platform}"
	fi

	build_cmd="$build_cmd -t $image ."
	$build_cmd

	# remove builder instance
#	if [ $platform == "all" ]; then
#		docker buildx rm multi-platform
#	fi
}

image=$(jq -r '.image' versions.json)
if [ "$platform" = arm64 ]; then
	image=${image}-arm64
fi


cat queries.yaml autofailover.yaml > autofailover_queries.yaml

# get queries.yaml
# wget https://raw.githubusercontent.com/prometheus-community/postgres_exporter/master/queries.yaml

build_image $image "${platform}"
