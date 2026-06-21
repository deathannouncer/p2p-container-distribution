#!/bin/sh
# Derives NODE_ID, ADVERTISE_ADDR, and PEERS from the pod's StatefulSet
# ordinal so the same container image works for any replica without
# per-pod config. Expects these env vars (set by k8s/statefulset.yaml):
#   STATEFULSET_NAME, SERVICE_NAME, NAMESPACE, REPLICAS
set -e

HOSTNAME_ORDINAL="${HOSTNAME##*-}"
NODE_ID="node-${HOSTNAME_ORDINAL}"
ADVERTISE_ADDR="${HOSTNAME}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local:8080"

PEERS=""
i=0
while [ "$i" -lt "${REPLICAS:-3}" ]; do
  if [ "$i" != "$HOSTNAME_ORDINAL" ]; then
    PEER_ADDR="${STATEFULSET_NAME}-${i}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local:8080"
    if [ -z "$PEERS" ]; then
      PEERS="node-${i}=${PEER_ADDR}"
    else
      PEERS="${PEERS},node-${i}=${PEER_ADDR}"
    fi
  fi
  i=$((i + 1))
done

export NODE_ID ADVERTISE_ADDR PEERS
echo "starting ${NODE_ID}: advertise=${ADVERTISE_ADDR} peers=${PEERS}"
exec /node "$@"
