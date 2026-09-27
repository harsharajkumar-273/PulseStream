# Local KEDA proof-of-concept cluster

`keda-hpa.yaml` (repo root) only makes sense inside a real Kubernetes cluster
with the KEDA operator installed and a `pulsestream-consumer-deployment` to
scale. Those don't exist in the docker-compose stack, so this directory is a
minimal, self-contained k8s deployment of the same pipeline (Postgres, Redis,
Redpanda, the consumer) used to prove that `keda-hpa.yaml`'s Kafka-lag trigger
actually scales the consumer's pod count, not just a docker-compose
simulation of it (see `benchmarks/lag_chart.js` for that faster, no-cluster
stand-in).

## Bring it up

Requires `kind`, `kubectl`, `helm`, and the images already built via
`docker-compose up --build` (they're loaded into the cluster, not pulled
from a registry).

```bash
kind create cluster --name pulsestream

helm repo add kedacore https://kedacore.github.io/charts
helm repo update
# Chart app version 2.21.0 segfaults (exit 139) on arm64 (e.g. Apple Silicon)
# kind nodes with no logs at all. 2.17.2 is confirmed stable there.
helm install keda kedacore/keda --namespace keda --create-namespace --version 2.17.2

kind load docker-image pulsestream-metrics-consumer:latest --name pulsestream

kubectl apply -f cluster/00-postgres.yaml
kubectl apply -f cluster/01-redis.yaml
kubectl apply -f cluster/02-redpanda.yaml
kubectl apply -f cluster/03-consumer-deployment.yaml
# wait for the above to be Ready (kubectl get pods), then:
kubectl apply -f ../keda-hpa.yaml
kubectl get scaledobject   # READY should flip to True within ~15s
```

If `READY` stays `False`, check `kubectl describe scaledobject pulsestream-consumer-scaler` —
the most likely cause is a stale Redpanda advertised address. Kafka clients (including
KEDA's own scaler) reconnect using whatever address the broker advertises in its metadata
response, not the bootstrap address you gave them, and **the KEDA operator resolves that
hostname from its own pod's namespace** (`keda`), not the target deployment's. That's why
`cluster/02-redpanda.yaml` advertises the fully-qualified
`redpanda.default.svc.cluster.local:29092` instead of the bare `redpanda:29092` that
docker-compose gets away with (there, both sides are the same "namespace").

## Generate a lag spike and watch it scale

```bash
kubectl apply -f cluster/04-load-job.yaml
kubectl get hpa -w   # KEDA creates/manages a HorizontalPodAutoscaler under the hood
kubectl get pods -l app=pulsestream-consumer -w
```

You should see `pulsestream-consumer-deployment` scale from 1 replica up to
3 (capped there by `metrics.raw`'s 3 partitions, regardless of
`maxReplicaCount: 10` — a 4th replica in the same consumer group would have
no partition left to own) as the Kafka consumer-group lag KEDA queries
directly from the broker climbs past `lagThreshold: "100"`, then scale back
down once the burst drains. In practice the scale-down took closer to the
default HPA scale-down stabilization window (~5 minutes) than
`cooldownPeriod: 30` alone, since KEDA drives a standard Kubernetes HPA
underneath and that HPA's own defaults still apply unless overridden via
`.spec.advanced.horizontalPodAutoscalerConfig.behavior`.

Measured run (300,000-event burst, `--count 300000` in `cluster/04-load-job.yaml`):

```
t=3s    lag/threshold ratio 300/100   desired=1  running_pods=3   (spinning up)
t=15s   lag/threshold ratio 100/100   desired=3  running_pods=3   (steady-state, draining)
t=42s   lag/threshold ratio   0/100   desired=3  running_pods=3   (drained)
~5 min later: back to 1 replica
```

All 300,000 events landed in Postgres (`kubectl exec deploy/postgres -- psql -U postgres -d pulsestream -c "SELECT count(*) FROM events;"`).

## Tear down

```bash
kind delete cluster --name pulsestream
```
