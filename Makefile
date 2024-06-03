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
export platform=amd64# make platform=arm64 build arm, make platform=all build and push arm64/amd64 image
export pgversion=all# make pgversion=12 only build postgresql 12 that in versions.json field. make pgversion="'13' '14' '15'" build postgresql 13 14 15.
export forcebuildimage=# local/remote
export namespace=radondb# used when make platform=all and no repositry namespace
export forcebuildhelm=0# 1 is force build helm package

### make log
# nohup make postgres-image &
# log file is nohup.out
### end

# batch replace version
# sed -i -e "s/\bdev\b/v1.2.1/g" exporterversions.json pgversions.json operatorversions.json

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
#operator-yaml: operator-image
operator-yaml:
	cp operatorversions.json platforms/kubernetes/postgres-operator/deploy/versions.json
	cp jq-template.awk platforms/kubernetes/postgres-operator/deploy/jq-template.awk
	cd platforms/kubernetes/postgres-operator/deploy/; awk -f jq-template.awk postgres-operator.yaml.template > postgres-operator.yaml
helm-package:
	if [ -s "./docs/postgres-operator-`jq -r '.version' helmversions.json`.tgz" -a "${forcebuildhelm}" != "1" ]; then echo "helm package exists, skiping ..." && exit 255; fi
	cp helmversions.json platforms/kubernetes/postgres-operator/deploy/versions.json
	cp jq-template.awk platforms/kubernetes/postgres-operator/deploy/jq-template.awk
	cd platforms/kubernetes/postgres-operator/deploy/; \
		/bin/rm -rf platforms/kubernetes/postgres-operator/deploy/postgres-operator/; \
		cp -r helm_template/postgres-operator/ ./postgres-operator; \
		mkdir -p postgres-operator/crds; \
		awk -f jq-template.awk postgres-operator.yaml.template | grep -B 99999 "crds is end" > postgres-operator/crds/postgres-crds.yaml; \
		awk -f jq-template.awk postgres-operator.yaml.template | grep -A 99999 "crds is end" > postgres-operator/templates/postgres-operator.yaml; \
		awk -f jq-template.awk postgres-operator/Chart.yaml.template > postgres-operator/Chart.yaml; \
		awk -f jq-template.awk postgres-operator/values.yaml.template > postgres-operator/values.yaml; \
		/bin/rm postgres-operator/Chart.yaml.template postgres-operator/values.yaml.template;
	helm package -d ./docs platforms/kubernetes/postgres-operator/deploy/postgres-operator/
	helm repo index --url https://radondb.github.io/multi-platform-postgresql/ ./docs
	/bin/rm -rf platforms/kubernetes/postgres-operator/deploy/postgres-operator/
exporter-image:
	cp exporterversions.json image/exporter/versions.json
	cp jq-template.awk image/exporter/jq-template.awk
	cd image/exporter; ./generate_image.sh

format:
	find ./ -path "./platforms/kubernetes/postgres-operator/postgres/*.py" | xargs yapf -i -vv --no-local-style --style pep8

depends:
	sudo pip install yapf paramiko kubernetes kopf
	docker run --privileged --rm tonistiigi/binfmt:master --install all
	@echo "TODO ubuntu: apt install jq curl"
	@echo "build helm-package need helm environment."
