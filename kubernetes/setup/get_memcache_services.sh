kubectl get svc -l "app = memcache" -o json | jq -r '.items[] | [.metadata.name,.spec.ports[0].targetPort] | @csv' | sed 's/"//g' | sed 's/,/:/' | paste -s -d,
