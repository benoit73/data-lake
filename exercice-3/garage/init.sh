#!/bin/sh
# Initialise Garage via son API d'administration (v2) :
# layout du nœud, import de la clé S3, création des buckets.
# Lancé automatiquement par le service "garage-init" ; idempotent.
set -eu

API=http://garage:3903/v2

call() {
  method=$1; path=$2; shift 2
  curl -sSf -X "$method" -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" \
       -H "Content-Type: application/json" "$API/$path" "$@"
}

echo "Attente de Garage..."
until call GET GetClusterStatus >/dev/null 2>&1; do sleep 1; done

layout=$(call GET GetClusterLayout)
if [ "$(echo "$layout" | jq '.roles | length')" -eq 0 ]; then
  node=$(call GET GetClusterStatus | jq -r '.nodes[0].id')
  version=$(( $(echo "$layout" | jq .version) + 1 ))
  echo "Layout : nœud $node (version $version)"
  call POST UpdateClusterLayout \
    -d "{\"roles\":[{\"id\":\"$node\",\"zone\":\"dc1\",\"capacity\":$GARAGE_CAPACITY_BYTES,\"tags\":[]}]}" >/dev/null
  call POST ApplyClusterLayout -d "{\"version\":$version}" >/dev/null
fi

if ! call GET "GetKeyInfo?id=$S3_ACCESS_KEY_ID" >/dev/null 2>&1; then
  echo "Import de la clé $S3_ACCESS_KEY_ID"
  call POST ImportKey \
    -d "{\"accessKeyId\":\"$S3_ACCESS_KEY_ID\",\"secretAccessKey\":\"$S3_SECRET_ACCESS_KEY\",\"name\":\"data-lake\"}" >/dev/null
fi

for bucket in $S3_BUCKETS; do
  id=$(call GET "GetBucketInfo?globalAlias=$bucket" 2>/dev/null | jq -r .id || true)
  if [ -z "$id" ]; then
    echo "Création du bucket $bucket"
    id=$(call POST CreateBucket -d "{\"globalAlias\":\"$bucket\"}" | jq -r .id)
  fi
  call POST AllowBucketKey \
    -d "{\"bucketId\":\"$id\",\"accessKeyId\":\"$S3_ACCESS_KEY_ID\",\"permissions\":{\"read\":true,\"write\":true,\"owner\":true}}" >/dev/null
done

echo "Garage prêt : buckets $S3_BUCKETS"
