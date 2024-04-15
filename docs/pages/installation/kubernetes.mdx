# Kubernetes

_Dolos_ can be implemented as a standalone Kubernetes `StatefulSet` resource. 

Please note that the amount of replicas is set to `1`. _Dolos_ doesn't have any kind of "coordination" between instances. Adding more than one replica will just create extra pipelines duplicating the same work.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dolos
data:
  daemon.toml: |-
    [upstream]
    # REDACTED: here goes your `upstream` configuration options

    [rolldb]
    # REDACTED: here goes your `rolldb` configuration options
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dolos
spec:
  template:
    spec:
      containers:
        - name: main
          image: ghcr.io/txpipe/dolos:latest

          # we mount the same volume that the main container uses as the source
          # for the Cardano node unix socket.
          volumeMounts:
            - mountPath: /var/dolos/db
              name: db
            - mountPath: /etc/dolos
              name: config
          resources:
            requests:
              memory: 1Gi
              cpu: 1
            limits:
              memory: 1Gi
      volumes:
        # an empty-dir to store your data. In a real scenario, this should be a PVC
        - name: db
          emptyDir: {}

        # a config map resource with Dolos' config, particular for your requirements
        - name: config
          configMap:
            name: config
```
