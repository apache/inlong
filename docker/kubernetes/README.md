## The Helm Chart for Apache InLong

### Prerequisites

- Kubernetes 1.10+
- Helm 3.0+
- A dynamic provisioner for the PersistentVolumes(`production environment`)

### Usage

#### Install

```shell
kubectl create namespace inlong
helm upgrade inlong --install -n inlong ./
```

#### Configuration

The configuration file is [values.yaml](values.yaml), and the following tables lists the configurable parameters of InLong and their default values.

|                                    Parameter                                     |     Default      |                                                                         Description                                                                          | 
|:--------------------------------------------------------------------------------:|:----------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|                                    `timezone`                                    | `Asia/Shanghai`  |                                                       World time and date for cities in all time zones                                                       |
|                               `images.pullPolicy`                                |  `IfNotPresent`  |                                                 Image pull policy. One of `Always`, `Never`, `IfNotPresent`                                                  |
|                         `images.<component>.repository`                          |                  |                                                          Docker image repository for the component                                                           |
|                             `images.<component>.tag`                             |     `latest`     |                                                              Docker image tag for the component                                                              |
|                             `<component>.component`                              |                  |                                                                        Component name                                                                        |
|                            `<component>.replicaCount`                            |       `1`        |                                                Replicas is the desired number of replicas of a given Template                                                |
|                        `<component>.podManagementPolicy`                         |  `OrderedReady`  |                PodManagementPolicy controls how pods are created during initial scale up, when replacing pods on nodes, or when scaling down                 |
|                            `<component>.annotations`                             |       `{}`       |                                 The `annotations` field can be used to attach arbitrary non-identifying metadata to objects                                  |
|                            `<component>.tolerations`                             |       `[]`       |                     Tolerations are applied to pods, and allow (but do not require) the pods to schedule onto nodes with matching taints                     |
|                            `<component>.nodeSelector`                            |       `{}`       |                 You can add the `nodeSelector` field to your Pod specification and specify the node labels you want the target node to have                  |
|                              `<component>.affinity`                              |       `{}`       |        Node affinity is conceptually similar to nodeSelector, allowing you to constrain which nodes your Pod can be scheduled on based on node labels        |
|                   `<component>.terminationGracePeriodSeconds`                    |       `30`       |                                              Optional duration in seconds the pod needs to terminate gracefully                                              |
|                             `<component>.resources`                              |       `{}`       |                                                Optionally specify how much of each resource a container needs                                                |
|                              `<component>.port(s)`                               |                  |                                                            The port(s) for each component service                                                            |
|                                `<component>.env`                                 |       `{}`       |                                                      Environment variables for each component container                                                      |
|       <code>\<component\>.probe.\<liveness&#124;readiness\>.enabled</code>       |      `true`      |                                                         Turn on and off liveness or readiness probe                                                          |
|  <code>\<component\>.probe.\<liveness&#124;readiness\>.failureThreshold</code>   |       `10`       |                                                         Minimum consecutive successes for the probe                                                          |
| <code>\<component\>.probe.\<liveness&#124;readiness\>.initialDelaySeconds</code> |       `10`       |                                                             Delay before the probe is initiated                                                              |
|    <code>\<component\>.probe.\<liveness&#124;readiness\>.periodSeconds</code>    |       `30`       |                                                                How often to perform the probe                                                                |
|                            `<component>.volumes.name`                            |                  |                                                                         Volume name                                                                          |
|                            `<component>.volumes.size`                            |      `10Gi`      |                                                                         Volume size                                                                          |
|                        `<component>.service.annotations`                         |       `{}`       |                                        The `annotations` field may need to be set when service.type is `LoadBalancer`                                        |
|                            `<component>.service.type`                            |   `ClusterIP`    |             The `type` field determines how the service is exposed. Valid options are `ClusterIP`, `NodePort`, `LoadBalancer` and `ExternalName`             |
|                         `<component>.service.clusterIP`                          |      `nil`       |                                  ClusterIP is the IP address of the service and is usually assigned randomly by the master                                   |
|                          `<component>.service.nodePort`                          |      `nil`       |                              NodePort is the port on each node on which this service is exposed when service type is `NodePort`                              |
|                       `<component>.service.loadBalancerIP`                       |      `nil`       |                            LoadBalancer will get created with the IP specified in this field when service type is `LoadBalancer`                             |
|                        `<component>.service.externalName`                        |      `nil`       | ExternalName is the external reference that kubedns or equivalent will return as a CNAME record for this service, requires service type to be `ExternalName` |
|                        `<component>.service.externalIPs`                         |       `[]`       |                        ExternalIPs is a list of IP addresses for which nodes in the cluster will also accept traffic for this service                        |
|                             `external.mysql.enabled`                             |     `false`      |                                         If not exists external MySQL, InLong will use the internal MySQL by default                                          |
|                            `external.mysql.hostname`                             |   `localhost`    |                                                                   External MySQL hostname                                                                    |
|                              `external.mysql.port`                               |      `3306`      |                                                                     External MySQL port                                                                      |
|                            `external.mysql.username`                             |      `root`      |                                                                   External MySQL username                                                                    |
|                            `external.mysql.password`                             |    `password`    |                                                                   External MySQL password                                                                    |
|                            `external.pulsar.enabled`                             |     `false`      |                                        If not exists external Pulsar, InLong will use the internal TubeMQ by default                                         |
|                           `external.pulsar.serviceUrl`                           | `localhost:6650` |                                                                 External Pulsar service URL                                                                  |
|                            `external.pulsar.adminUrl`                            | `localhost:8080` |                                                                  External Pulsar admin URL                                                                   |

> The components include `agent`, `audit`, `dashboard`, `dataproxy`, `manager`, `tubemq-manager`, `tubemq-master`, `tubemq-broker`, `zookeeper` and `mysql`.

#### Uninstall

```shell
helm uninstall inlong -n inlong
```

You can delete all `PVC ` if any persistent volume claims used, it will lose all data.

```shell
kubectl delete pvc -n inlong --all
```
