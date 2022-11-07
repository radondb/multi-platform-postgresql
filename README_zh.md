![LOGO](https://github.com/radondb/radondb-clickhouse-kubernetes/tree/main/document/_images/logo_radondb.png)
 
> [English](README.md) | 中文

## 什么是 RadonDB PostgreSQL

[PostgreSQL](https://www.postgresql.org/) 世界上最先进的开源关系数据库。

[RadonDB PostgreSQL](https://github.com/radondb/multi-platform-postgresql) 具备高可用、高扩展性、QingCloud 推出的开源的支持在 kubernetes/物理机/虚拟机 上部署的 PostgreSQL Operator。

## 快速开始

👀 本教程主要演示如何在 Kubernetes 上部署 RadonDB PostgreSQL 集群(Operator)。

## 部署准备

📦 已准备可用 Kubernetes 集群。

## 部署步骤

### 步骤 1: 部署 RadonDB PostgreSQL Operator

请从以下两种方式种选择一种部署 Radondb PostgreSQL Operator.
1. 通过 helm 部署
2. 通过 Kubectl 部署

#### a. 通过 helm 部署

##### 1) 添加 Helm 仓库

```plain
helm repo add radondb-postgresql https://radondb.github.io/multi-platform-postgresql/
```

##### 2) 部署 Operator

创建一个名为 `radondb-postgres-operator` 的 [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)。

```plain
helm install demo radondb-postgresql/postgres-operator
```
> **说明**

> 在这一步骤中默认将同时创建集群所需的 [CRD](https://kubernetes.io/zh/docs/concepts/extend-kubernetes/api-extension/custom-resources/)。 

#### b. 通过 Kubectl 部署

##### 1) 创建一个名为 `radondb-postgres-operator` 的 namespace

```plain
kubectl create ns radondb-postgres-operator
```

##### 2) 部署 Operator

```plain
kubectl apply -f https://raw.githubusercontent.com/radondb/multi-platform-postgresql/V3.0.0/platforms/kubernetes/postgres-operator/deploy/postgres-operator.yaml
```

### 步骤 2: 部署 RadonDB PostgreSQL 集群

执行以下指令，以默认参数为 CRD `postgresqls.postgres.radondb.io` 创建一个实例，即创建 RadonDB PostgreSQL 集群。

```plain
curl https://raw.githubusercontent.com/radondb/multi-platform-postgresql/V3.0.0/platforms/kubernetes/postgres-operator/deploy/postgresql.yaml | sed -e "s/image: /image: radondb\//g" > postgresql.yaml
kubectl apply -f postgresql.yaml
```

## 协议

查看 [LICENSE](License) 获取更多信息。

## 欢迎加入社区话题互动 ❤️

😊 社区官网：[https://radondb.com](https://radondb.com)

😁 社区论坛：请加入 KubeSphere 开发者论坛 [RadonDB](https://kubesphere.com.cn/forum/t/RadonDB) 板块。

😆 社区公众号：RadonDB 开源社区

🦉 社区微信群：请添加群助手 radondb 邀请进群

如有任何关于 RadonDB PostgreSQL 的 Bug、问题或建议，请在 GitHub 提交 [issue](https://github.com/radondb/multi-platform-postgresql/issues) 或[论坛](https://kubesphere.com.cn/forum/t/RadonDB)反馈。

