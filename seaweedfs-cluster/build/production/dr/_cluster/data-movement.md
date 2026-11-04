# Enterprise Data Movement: primary -> dr
Configure on the primary Admin UI (http://10.10.6.11:23646).

Target  : name=primary-to-dr  type=SeaweedFS Cluster  filer gRPC=10.20.4.11:18888  mTLS=on
          (fallback gRPC endpoints: 10.20.4.12:18888, 10.20.4.13:18888)
Policies:
  - name=buckets-archive-primary-to-dr source=/buckets/archive dest=/buckets/archive mode=CONTINUOUS initial_snapshot=ON delete_propagation=ON bandwidth_cap=<set>
Backup policy (separate target, e.g. object-lock S3 or 3rd cluster):
  - mode=BACKUP delete_propagation=OFF retention=<per legal hold policy>
Enable plugin job type `data_movement`; run >= 2 workers on the primary admin host.
OSS fallback (tested in the lab): seaweed-filer-sync service on the dr side
  weed filer.sync -a <primary filer> -b <dr filer> -a.path=P -b.path=P -isActivePassive
