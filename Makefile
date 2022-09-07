#-------------------------------------------------------------------------
#
# Makefile for multi-platform-postgresql
#
# auther:
# 	"张连壮" <lianzhuangzhang@yunify.com>
# 	"颜博" <jerryyan@yunify.com>
#
# Date:
# 	2022.06
#
#-------------------------------------------------------------------------
FILEDESC = "multi platform for computer/kubernetes postgresql"

#docker_version ?= ""
export platform=amd64# make platform=arm64 build arm
export pgversion=all# make pgversion=12 only build postgresql 12 that in versions.json field.
export forcebuildimage=0# make forcebuildimage=1 even though the image exists, it also compiles.

k8s: download postgres-image operator-yaml operator-image exporter-image
all: k8s

download:
	source_code/download.sh ${PWD}/source_code
postgres-image: download
	cp pgversions.json image/postgresql/versions.json
	cp jq-template.awk image/postgresql/jq-template.awk
	cd image/postgresql; ./generate_image.sh
operator-image:
	cp operatorversions.json image/postgres-operator/versions.json
	cd image/postgres-operator; ./generate_image.sh
operator-yaml: operator-image
	cp operatorversions.json platforms/kubernetes/postgres-operator/deploy/versions.json
	cp jq-template.awk platforms/kubernetes/postgres-operator/deploy/jq-template.awk
	cd platforms/kubernetes/postgres-operator/deploy/; awk -f jq-template.awk postgres-operator.yaml.template > postgres-operator.yaml
exporter-image:
	cp exporterversions.json image/exporter/versions.json
	cd image/exporter; ./generate_image.sh

format:
	find ./ -path "./platforms/kubernetes/postgres-operator/postgres/*.py" | xargs yapf -i -vv

depends:
	sudo pip install yapf paramiko kubernetes kopf
	docker run --privileged --rm tonistiigi/binfmt --install all
	echo "TODO ubuntu: apt install jq"
