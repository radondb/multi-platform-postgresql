#!/usr/bin/env bash
set -Eeo pipefail

function build_release() {
	version=$1
	version=$(echo ${version} | sed 's/\./-/g')

	cd ../postgres
	/bin/rm -rf ${version}
	mkdir ${version}
	cp *.py ${version}/
	sed -i -e "s/API_VERSION = \"[^\"]*\"/API_VERSION = \"${version}\"/" ${version}/constants.py
	cd -
}

versions=$(jq -r '.crdVersions[]' versions.json)
for version in $versions; do
	build_release $version
done