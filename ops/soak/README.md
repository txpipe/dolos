# dolos-soak

A Helm chart that soaks a dolos build on real traffic: one instance that
restores a stele, follows the tip and can join a serving pool. Any build is
soaked from a values file alone; the chart assumes no cluster, network or
registry.

```sh
helm upgrade --install <release> ops/soak/chart \
  --namespace <namespace> --values <instance values file>
```

## What it deploys

- **ConfigMap `<name>-config`** holding `dolos.toml`, taken verbatim from
  `dolosToml` and mounted at `/etc/config`.
- **StatefulSet `<name>`**, one replica, with a `data` volume claim mounted at
  `/var/data`. The claim is kept when the StatefulSet is deleted or scaled
  down.
  - The `init` container restores the stele with `dolos bootstrap stelae
    --continue`, then writes `/var/data/<network>/.stele-restored`. A
    recreated pod finds the marker and skips the restore; an interrupted
    restore resumes from its progress file. It ends by printing `dolos data
    summary`.
  - The `main` container runs `dolos daemon`.
  - Startup, readiness and liveness probes pass while the latest block from
    minibf's `/blocks/latest` is at most `freshness.maxAgeSeconds` old. The
    image ships no HTTP client, so the probe makes the request with bash's
    `/dev/tcp` and parses the JSON with `grep`; it installs nothing.

The chart creates no Service: `serviceName` names one that exists already.

## Values

| Value | Default | Meaning |
| --- | --- | --- |
| `name` | required | Instance name: names the StatefulSet and ConfigMap and selects the pod. |
| `network` | required | Network the instance follows; places the restore marker. |
| `image.repository` | `ghcr.io/txpipe/dolos` | Image repository. |
| `image.digest` | required | `sha256:<64 hex>`. A soak judges one exact build, so a tag is refused. `init` and `main` run the same image. |
| `image.pullPolicy` | `IfNotPresent` | Pull policy for both containers. |
| `dolosToml` | required | The instance's `dolos.toml`, verbatim. Keep `storage.path` under `/var/data`. |
| `stele.source` | required | `oci://HOST/PATH` or `file://DIR` to restore from. |
| `stele.point` | `latest` | Which stele to read: `latest` or an epoch. |
| `stele.insecure` | `false` | Plain HTTP to the registry; for an in-cluster mirror only. |
| `storage.size` | required | Data volume size, e.g. `500Gi`. |
| `storage.className` | cluster default | Storage class of the data volume. |
| `podLabels` | `{}` | Pool membership labels, applied to the pod template only. |
| `labels` | `{}` | Extra labels on the StatefulSet's own metadata. |
| `serviceName` | `name` | Headless Service the StatefulSet names. |
| `extraEnv` | `[]` | Extra env for `main`, as a Kubernetes env list, e.g. `_RJEM_MALLOC_CONF`. |
| `ports` | grpc 50051, minibf 3000, trp 8000, kupo 1442 | Ports of `main`; must match the listen addresses in `dolosToml`. |
| `freshness.port` | `3000` | minibf port the probe reads on `127.0.0.1`. |
| `freshness.maxAgeSeconds` | `600` | Oldest acceptable latest block. |
| `probes.startup` | delay 10s, every 30s, timeout 5s, 200 failures | Startup probe timing; covers the catch-up after a restore. |
| `probes.readiness` | delay 10s, every 30s, timeout 5s, 5 failures | Readiness probe timing. |
| `probes.liveness` | delay 45s, every 30s, timeout 5s, 10 failures | Liveness probe timing. |
| `resources.init`, `resources.main` | `{}` | Container resources. |
| `nodeSelector`, `tolerations` | empty | Placement. |
| `terminationGracePeriodSeconds` | `180` | Time for the daemon to stop. |
| `automountServiceAccountToken` | `true` | Mount the service account token. |

Rendering fails with `<value> is required` when `name`, `network`,
`image.digest`, `dolosToml`, `stele.source` or `storage.size` is missing,
and when `image.digest` is not a digest. `helm lint` does not enforce
`required`, so lint with an instance values file and check required values
with `helm template`. `test.sh` runs these checks.

## Pool labels stay off the StatefulSet

A serving pool selects pods, so `podLabels` go on the pod template and
nowhere else. Tooling that surveys StatefulSets by a pool label, such as a
canary rollout choosing which instance to patch, must never find a soak
instance. Rendering fails if `labels` carries `cardano.demeter.run/network`.

## The example

`chart/values.example.yaml` reproduces the `cardano-mainnet-stelae` soak
instance that ran on m2 from a hand-written manifest, installed as that
release in `ftr-dolos-m1`. The render carries the manifest's `dolos.toml`
byte for byte and its StatefulSet field for field, except for these
intentional differences:

- **Image:** pinned by digest (`sha-4866b97`'s index digest) instead of the
  tag.
- **Names:** the ConfigMap is `cardano-mainnet-stelae-config`, not
  `configs-cardano-mainnet-stelae`.
- **Labels:** the StatefulSet and pod template add the
  `app.kubernetes.io/*` labels, and the StatefulSet `helm.sh/chart`. The
  selector is `app.kubernetes.io/name` and `app.kubernetes.io/instance`
  instead of `demeter.run/instance`, which stays as an ordinary label.
- **Main command:** `dolos -c /etc/config/dolos.toml daemon` directly. The
  manifest installed `curl` and `jq` with `apt-get` at container start for
  its probes.
- **Probes:** the same freshness check and timings, made with bash, `grep`
  and `timeout` from the image instead of `curl` and `jq`.

A StatefulSet's selector is immutable, so the chart cannot adopt a
StatefulSet the manifest created; that one has been removed from the
cluster.
