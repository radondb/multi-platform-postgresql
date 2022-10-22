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
	pf=$3

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

	cd "$dir"
	echo "build docker image $image ..."
	docker builder build --no-cache -t $image --platform $pf .
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
		build_image "${version}/${platform}" $image "linux/${platform}"
#		if [ `jq '.[env.version]."arches" | contains(["aarch64"])' versions.json` == "true" ]; then
#			build_image "${version}/aarch64" "${image}-aarch64" "linux/arm64"
#		fi
	done
done
