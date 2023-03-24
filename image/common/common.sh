#!/usr/bin/env bash
set -Eeo pipefail

function pre_build_image()
{
	image=$1

	tempImage=($(echo $image | tr "/" " "))
	if [ ${#tempImage[@]} == 1 ]; then
		tempImage="$namespace/$image"
	else
		tempImage="$image"
	fi
	# get imageName and imageTag
	temp=($(echo $tempImage | tr ":" " "))
	imageName=${temp[0]}
	imageTag=${temp[1]}
	# check local and repository image
	repositoryImageExists=$(curl --silent -f --head -lL https://hub.docker.com/v2/repositories/$imageName/tags/$imageTag/ > /dev/null && echo "success" || echo "failed")
	localImageExists=$( docker image ls --format "{{.Repository}}:{{.Tag}}" | awk -v aaa=$image '{print $0} END{print aaa}' | grep -c $image )

	# check local image
	if [ "$localImageExists" -ne 1 ]; then
		if [ "$forcebuildimage" ]; then
			echo "docker image $image exists, rebuilding the image ..."
		else
			echo "docker image $image exists, skiping ..."
			return 255
		fi
	fi

	# check repository image
	if [ "$repositoryImageExists" == "success" ]; then
		if [ "$forcebuildimage" != "remote" ]; then
			echo "docker image $imageName:$imageTag exists on dockerhub, skiping ... "
			return 255
		else
			echo "docker image $imageName:$imageTag exists on dockerhub, rebuild image ..."
		fi
	fi

	echo "pre_build_image success!"
	return 0
}

function get_build_cmd() {
	image=$1
	platform=$2

	build_cmd="docker buildx build --no-cache"
	#docker_version=$(docker version --format '{{index (split .Server.Version ".") 0}}')
	if [ $platform == "all" ]; then
		builder_exists=$(docker buildx ls | awk '{if ($1=="multi-platform") print $1}')
		if [ "$builder_exists" ]; then
			docker buildx rm multi-platform
		fi
		# create a new builder instance
		docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64 > /dev/null

		temp=($(echo $image | tr "/" " "))
		if [ ${#temp[@]} == 1 ]; then
			image="$namespace/$image"
		fi

		build_cmd="$build_cmd --push --platform linux/amd64,linux/arm64"
	else
		build_cmd="$build_cmd -o type=docker --platform linux/${platform}"
	fi

	echo "$build_cmd -t $image ."

	# remove builder instance
#	if [ $platform == "all" ]; then
#		docker buildx rm multi-platform
#	fi
}
