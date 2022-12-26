#!/usr/bin/env bash
set -Eeo pipefail

#if [ "$#" -eq 0 ]; then
#	versions="$(jq -r 'keys | map(@sh) | join(" ")' versions.json)"
#else
#	versions=$@
#fi
if [ "$pgversion" = all ]; then
	versions="$(jq -r 'keys | map(@sh) | join(" ")' versions.json)"
else
	versions=$pgversion
fi
eval "set -- $versions"

build_image()
{
	dir=$1
	image=$2
	platform=$3

	/bin/rm -rf "$dir"
	mkdir -p "$dir"
	echo "processing $dir ..."

	image_exists=$( docker image ls --format "{{.Repository}}:{{.Tag}}" | awk -v aaa=$image '{print $0} END{print aaa}' | grep -c $image )
	if [ "$image_exists" -ne 1 ]; then
		if [ "$forcebuildimage" = 1 ]; then
			echo "docker image $image exists, rebuilding the image ..."
		else
			echo "docker image $image exists, skiping ..."
			return
		fi
	fi

	echo "generate Dockerfile ..."
	awk -f jq-template.awk Dockerfile.template > "$dir/Dockerfile"
	cp -a docker-entrypoint.sh "$dir/"
	cp -a pgtools "$dir/"

	echo "copy code ..."
	cp -r ../../source_code/postgresql "$dir/"
	cp -r ../../source_code/pg_auto_failover "$dir/"
	cp -r ../../source_code/pg_dirtyread "$dir/"
	cp -r ../../source_code/citus "$dir/"
	cp -r ../../source_code/postgis "$dir/"

	cd "$dir"
	echo "build docker image $image ..."
	build_cmd="docker buildx build --no-cache"

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
	cd -
}


for version; do
	#arch="$(uname -m)"
	#export arch
	export version
	#if [ `jq '.[env.version]."arches" | contains([env.arch])' versions.json` == "true" ]; then
	for ((index=0;index<$(jq -r '.[env.version].minor | length' versions.json);index++)); do
		export index
		image=$(jq -r '.[env.version].name' versions.json):$(jq -r '.[env.version].major' versions.json).$(jq -r ".[env.version].minor[$index]" versions.json)-$(jq -r '.[env.version].version' versions.json)
		if [ "$platform" = arm64 ]; then
			image=${image}-arm64
		fi
		build_image "${version}/${platform}" $image "${platform}"
#		if [ `jq '.[env.version]."arches" | contains(["aarch64"])' versions.json` == "true" ]; then
#			build_image "${version}/aarch64" "${image}-aarch64" "linux/arm64"
#		fi
	done
done
