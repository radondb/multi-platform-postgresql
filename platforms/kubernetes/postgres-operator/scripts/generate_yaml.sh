#!/usr/bin/env bash
set -Eeo pipefail

export operator_file="../deploy/postgres-operator.yaml"

function build_release() {
	file=$1
	echo "---" >> "$file"
	awk -f jq-template.awk postgresql-crd.yaml.template >> "$file"
}

awk -f jq-template.awk postgres-operator.yaml.template > $operator_file

# add new version
for ((index=0;index<$(jq -r '.crdVersions | length' versions.json);index++)); do
	export index
	build_release $operator_file
done
