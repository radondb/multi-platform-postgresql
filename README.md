![LOGO](.document/images/logo_radondb.png)
 
> English | [中文](README_zh.md)

## What is RadonDB PostgreSQL

[PostgreSQL](https://www.postgresql.org/) The World's Most Advanced Open Source Relational Database.

[RadonDB PostgreSQL](https://github.com/radondb/multi-platform-postgresql) High availability, High scalability, QingCloud Publish Open Source, PostgreSQL Operator On K8s and Machine.

RadonDB PostgreSQL Kubernetes supports [Kubernetes](https://kubernetes.io) or machine platforms.

## QuickStarts

👀 This tutorial demonstrates how to deploy a RadonDB PostgreSQL cluster (Operator) on Kubernetes.

## Preparation

📦 Prepare a Kubernetes cluster.

## Steps

### Step 1: Deploy RadonDB PostgreSQL Operator

Please select a method to deploy RadonDB PostgreSQL Operator.
1. By Helm
2. By Kubectl

#### a. By Helm

##### 1) Add a Helm repository.

```plain
helm repo add radondb-postgresql https://radondb.github.io/multi-platform-postgresql/
```

##### 2) Install Operator.

Create a [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) named `radondb-postgres-operator`.

```plain
helm install demo radondb-postgresql/postgres-operator
```
> **Notice**

> This step also creates the [CRD](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) required by the cluster.

#### b. By Kubectl

##### 1) Create `radondb-postgres-operator` namespace

```plain
kubectl create ns radondb-postgres-operator
```

##### 2) Install Operator.

```plain
kubectl apply -f https://raw.githubusercontent.com/radondb/multi-platform-postgresql/V3.0.0/platforms/kubernetes/postgres-operator/deploy/postgres-operator.yaml
```

### Step 2: Deploy a RadonDB PostgreSQL Cluster.

Run the following command to create an instance of the `postgresqls.postgres.radondb.io` CRD and thereby create a RadonDB PostgreSQL cluster by using the default parameters.

```plain
curl https://raw.githubusercontent.com/radondb/multi-platform-postgresql/V3.0.0/platforms/kubernetes/postgres-operator/deploy/postgresql.yaml | sed -e "s/image: /image: radondb\//g" > postgresql.yaml
kubectl apply -f postgresql.yaml
```

## License

See [LICENSE](License) for more information.

## Welcome to join us ❤️

😊 Website: [https://radondb.com/](https://radondb.com/en/)

😁 Forum: Please join the [RadonDB](https://kubesphere.com.cn/forum/t/RadonDB) section of kubesphere Developer Forum.

🦉 Community WeChat group: Please add the group assistant **radondb** to invite you into the group.

For any bugs, questions, or suggestions about RadonDB multi-platform-postgresql, please create an [issue](https://github.com/radondb/multi-platform-postgresql/issues) on GitHub or feedback on the [forum](https://kubesphere.com.cn/forum/t/RadonDB).

