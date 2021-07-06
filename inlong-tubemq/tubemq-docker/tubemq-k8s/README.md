### the Helm Chart for TubeMQ
This helm chart provides an implementation of the TubeMQ StatefulSet, include Master/Broker/ZooKeeper services.

#### Prerequisites
- Kubernetes 1.10+
- Helm 3.0+
- A dynamic provisioner for the PersistentVolumes(`production environment`)

#### Installing the Chart
You can install the chart with the release name `tubemq`  as below.
```
$ kubectl create namespace tubemq
$ helm upgrade tubemq --install -n tubemq ./
```
Installed Components
You can use `kubectl get po -n tubemq`  to view all of the installed components.
```
NAME                     READY   STATUS    RESTARTS   AGE
tubemq-k8s-broker-0      1/1     Running   0          2m36s
tubemq-k8s-broker-1      1/1     Running   0          54s
tubemq-k8s-broker-2      1/1     Running   0          30s
tubemq-k8s-master-0      1/1     Running   0          2m36s
tubemq-k8s-zookeeper-0   1/1     Running   0          2m36s
tubemq-k8s-zookeeper-1   1/1     Running   0          116s
tubemq-k8s-zookeeper-2   1/1     Running   0          86s
```
#### Configuration
The following table lists the configurable parameters of the `tubemq-k8s` chart and their default values.
| Parameter | Description | Default |
| :--: | :--:| :--: |
| images.tubemq_all.repository  | Container image repository |  apachetubemq/tubemq-all   |
| images.tubemq_all.tag | Container image tag |  latest   |
| images.tubemq_all.pullPolicy | Container pull policy | `IfNotPresent   ` |
| volumes.persistence  | Using  Persistent  |  false   |
| volumes.storageClassName | Persistent volume storage class | `"local-storage"` |
| affinity.anti_affinity | Container image repository |  false   |

You can specify each parameter using the `--set key=value[,key=value]` argument to helm install.For example,
```
$ helm upgrade tubemq --install --set master.ports.webNodePort=8081 -n tubemq ./
```

#### Uninstall
```
$ helm uninstall tubemq -n tubemq
```
You can delete all `PVC ` if  any persistent volume claims used, it will lost all data.
```
$ kubectl delete pvc -n tubemq --all
```