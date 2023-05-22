#!/usr/bin/env bash
set -Eeo pipefail

source ../common/common.sh
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

	# image check
	pre_build_image "$image" || error=true
	if [ "$error" ]; then
		return
	fi

	# get queries.yaml
	# wget https://raw.githubusercontent.com/prometheus-community/postgres_exporter/master/queries.yaml
	echo "generate queries.yaml ..."
	awk -f jq-template.awk queries.template autofailover.yaml > "$dir/autofailover_queries.yaml"
	awk -f jq-template.awk queries.template > "$dir/queries.yaml"
	cp Dockerfile "$dir/"

	cd "$dir"
	echo "build docker image $image ..."
	echo "notice: build all image need docker login. "

	build_cmd=$(get_build_cmd "$image" "$platform")
	$build_cmd

	cd -
}

for version; do
	export version
	image=$(jq -r '.[env.version].name' versions.json):$(jq -r '.[env.version].major' versions.json)-$(jq -r '.[env.version].version' versions.json)
	if [ "$platform" = arm64 ]; then
		image=${image}-arm64
	fi
	build_image "${version}/${platform}" $image "${platform}"
done
