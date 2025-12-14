# Dagster Deployment Documentation

**Date Deployed:** 2025-12-14  
**Version:** Dagster 1.12.6  
**Status:** ✅ Running  
**Namespace:** dagster

> **Note:** This is the detailed deployment documentation. For a quick start guide, see [README.md](./README.md).

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Kubernetes Resources](#3-kubernetes-resources)
4. [Configuration](#4-configuration)
5. [Networking](#5-networking)
6. [Health Checks](#6-health-checks)
7. [Credentials](#7-credentials)
8. [Data Management](#8-data-management)
9. [Troubleshooting](#9-troubleshooting)
10. [Maintenance](#10-maintenance)
11. [References](#11-references)
12. [Changelog](#12-changelog)

---

## 1. Overview

### 1.1 Purpose

This deployment provides a Kubernetes-based Dagster orchestration platform for data engineering workflows. The primary use case is crypto market data extraction and transformation, but the architecture supports any batch-processing pipeline.

**Business Use Case:**
- Periodically pull financial data from crypto exchange APIs (Binance, ByBit, Gate.io)
- Clean and transform data for analysis
- Create dashboards for trading decision support
- Prototype trading automation strategies

**Why Dagster?**
- **Battle-tested**: Proven in production environments
- **Developer-friendly**: Excellent UI with detailed pipeline monitoring
- **Flexible deployment**: Supports multiple code locations for separation of concerns
- **High Availability**: Can scale to 3+ replicas when needed
- **Active community**: Strong GitHub community and Slack support

### 1.2 Design Decisions

**Current Architecture:**
- **Single replica**: No HA (acceptable risk for homelab/prototyping)
- **Shared PostgreSQL**: Reuses existing cluster PostgreSQL (lower operational overhead)
- **External MinIO**: Raw data stored in LXC-based MinIO (S3-compatible)
- **No external access**: UI accessible only via internal network

**Trade-offs:**
- ❌ **Single replica risk**: Pipeline downtime if pod fails (mitigated by fast restart)
- ❌ **Shared PostgreSQL load**: Increased database load (acceptable for <1000 runs/day)
- ✅ **Simplified operations**: Fewer components to manage
- ✅ **Cost-effective**: No additional infrastructure required

**Migration Paths:**

When scaling beyond prototyping, consider:

1. **Dedicated PostgreSQL** - When shared instance shows performance issues or reaches 10GB
2. **Specialized time-series DB** - QuestDB or TimescaleDB when data volume exceeds 100GB
3. **Streaming architecture** - Kafka + Flink when job frequency exceeds 1/minute
4. **High Availability** - 3-replica Dagster instance when trading automation goes live

---

## 2. Prerequisites

### 2.1 Dependencies

| Dependency | Version | Purpose | Deployment Method |
|------------|---------|---------|-------------------|
| Kubernetes | 1.24+ | Container orchestration | Talos Linux (6-node cluster) |
| PostgreSQL | 17.6.0 | Dagster metadata storage | Bitnami Helm chart |
| MinIO | Latest | Raw JSON storage | LXC container |
| MetalLB | Latest | LoadBalancer for bare-metal | Helm chart |
| Traefik | Latest | Ingress controller | Helm chart |
| Sealed Secrets | Latest | Credential encryption | Official controller |

**PostgreSQL Configuration:**
- **Host:** `postgresql.database.svc.cluster.local:5432`
- **Database:** `dagster` (created separately in shared instance)
- **User:** `dagster` (with permissions for `dagster` database only)
- **Storage:** 10Gi PVC backed by Longhorn (3-way replication)

**MinIO Configuration:**
- **Host:** `minio.lxc.local:9000` (LXC container, not in K8s)
- **Bucket:** `crypto-raw-data`
- **Access:** S3-compatible API

### 2.2 Required Secrets

Secrets are managed via Sealed Secrets controller:

```bash
# Create sealed secret
kubectl create secret generic postgres-secrets \
  --from-literal=postgresql-password='<strong-password>' \
  --namespace dagster \
  --dry-run=client -o yaml | \
  kubeseal -o yaml > overlays/prod/sealed-secret.yaml
```

**Secret Keys:**
- `postgresql-password` - Password for `dagster` PostgreSQL user

**Security Notes:**
- Original unsealed secrets must NEVER be committed to Git
- Sealed secrets can be safely committed (encrypted with cluster public key)
- Secrets are automatically decrypted by Sealed Secrets controller when applied

### 2.3 Storage Requirements

Dagster instance is **stateless** - no persistent storage required.

| Component | Storage Type | Size | Backend | Notes |
|-----------|--------------|------|---------|-------|
| Run History & Metadata | Database | N/A | PostgreSQL (`dagster` database) | ~50MB/month at 1440 runs/day |
| Compute Logs | Database | N/A | PostgreSQL (text in run tables) | Retained 7 days, then purged |
| Raw API Data | Object Storage | Unbounded | MinIO (LXC) | Historical crypto price data |

**Storage Architecture:**
- ✅ **No PVCs needed** for Dagster pods (fully stateless)
- ✅ **PostgreSQL handles all persistence** (run state, logs, schedules)
- ⚠️ **Compute logs in PostgreSQL** increases DB size but simplifies architecture

---

## 3. Kubernetes Resources

### 3.1 Resource Summary

| Resource Type | Name | Purpose | Created By |
|---------------|------|---------|------------|
| Namespace | `dagster` | Isolation | Manual (base/namespace.yaml) |
| Deployment | `dagster-dagster-webserver` | UI & API | Helm chart |
| Deployment | `dagster-dagster-daemon` | Scheduler | Helm chart |
| Service | `dagster-dagster-webserver` | Webserver endpoint | Helm chart |
| Service | `dagster-dagster-daemon` | Daemon endpoint | Helm chart |
| IngressRoute | `dagster-ingress` | External access | Manual (base/ingressroute.yaml) |
| SealedSecret | `postgres-secrets` | PostgreSQL credentials | Manual (overlays/prod/) |

**Note:** Helm chart naming convention is `{releaseName}-{chartName}-{componentName}`. Since `releaseName=dagster` and `chartName=dagster`, services are named `dagster-dagster-*`.

### 3.2 Resource Locations

```
kubernetes/
└── apps/
    └── dagster/
        ├── base/
        │   ├── kustomization.yaml      # Helm chart + config
        │   ├── namespace.yaml          # Namespace definition
        │   └── ingressroute.yaml       # Traefik ingress
        └── overlays/
            └── prod/
                ├── kustomization.yaml  # Environment patches
                └── sealed-secret.yaml  # Encrypted credentials
```

---

## 4. Configuration

### 4.1 Environment Variables

Dagster pods receive configuration via Helm values in `kustomization.yaml`:

```yaml
dagster-webserver:
  env:
    - name: DAGSTER_PG_PASSWORD
      valueFrom:
        secretKeyRef:
          name: postgres-secrets
          key: postgresql-password

dagsterDaemon:
  env:
    - name: DAGSTER_PG_PASSWORD
      valueFrom:
        secretKeyRef:
          name: postgres-secrets
          key: postgresql-password
```

### 4.2 Important Settings

| Setting | Value | Rationale |
|---------|-------|-----------|
| Webserver Replicas | 1 | Homelab environment, HA not required |
| Daemon Replicas | 1 | Only one daemon should run (leader election built-in) |
| Max Concurrent Runs | 6 | Limit parallel job execution to avoid resource exhaustion |
| PostgreSQL Connection Pool | Default | Helm chart defaults sufficient for <100 concurrent runs |

**Resource Limits:**

```yaml
dagster-webserver:
  resources:
    limits:
      cpu: 500m
      memory: 512Mi
    requests:
      cpu: 100m
      memory: 128Mi
```

### 4.3 Job Execution Configuration

**Execution Frequency:**

| Job Type | Frequency | Rationale |
|----------|-----------|-----------|
| Market Data Extraction | Every 1 minute | Capture price movements |
| Data Transformation | Every 5 minutes | Batch process accumulated data |
| Dashboard Updates | Every 15 minutes | Balance freshness vs. query load |

**API Rate Limits:**

| Exchange | Rate Limit | Our Usage | Safety Margin |
|----------|------------|-----------|---------------|
| Binance | 1200 req/min | ~1 req/min | 1200x headroom |
| ByBit | 120 req/min | ~1 req/min | 120x headroom |
| Gate.io | 900 req/min | ~1 req/min | 900x headroom |

**Resource Implications:**
- **1 extraction run/min** = 1,440 runs/day = ~43,000 runs/month
- **PostgreSQL growth:** ~50MB/month (run metadata + logs)
- **Acceptable for homelab** - No scaling needed at this frequency

---

## 5. Networking

### 5.1 Access URLs

| URL | Purpose | Access Method |
|-----|---------|---------------|
| `http://dagster.homelab.lan` | Dagster UI | Traefik IngressRoute (internal network only) |
| `postgresql.database.svc.cluster.local:5432` | PostgreSQL | Kubernetes Service (ClusterIP) |

**DNS Resolution:**
- Wildcard DNS: `*.homelab.lan` → `192.168.0.2` (MetalLB VIP)
- Configured in Pi-hole: `/etc/dnsmasq.d/02-homelab.conf`

### 5.2 Internal Services

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| `dagster-dagster-webserver` | 80 | HTTP | Web UI and API |
| `dagster-dagster-daemon` | (no port) | - | Background scheduler |
| Code Location Services | 3030 | gRPC | User code execution |

### 5.3 IngressRoute Configuration

**Traefik CRD:**

```yaml
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: dagster-ingress
  namespace: dagster
spec:
  entryPoints:
    - web
  routes:
    - match: Host(`dagster.homelab.lan`)
      kind: Rule
      services:
        - name: dagster-dagster-webserver  # Must match Helm service name
          port: 80
```

**Critical:** Service name must be `dagster-dagster-webserver`, not just `dagster`. See [Section 9.1](#91-common-issues) for troubleshooting.

---

## 6. Health Checks

### 6.1 Endpoints

| Type | Endpoint | Expected Response |
|------|----------|-------------------|
| Liveness | `/dagit_info` | HTTP 200 + JSON response |
| Readiness | `/server_info` | HTTP 200 + JSON response |

### 6.2 Probe Configuration

**Helm Chart Defaults:**

```yaml
dagsterWebserver:
  livenessProbe: {}  # Disabled by default
  startupProbe:
    enabled: false
  readinessProbe:
    httpGet:
      path: /server_info
      port: 80
    periodSeconds: 20
    timeoutSeconds: 10
    successThreshold: 1
    failureThreshold: 3
```

**Why liveness probe disabled:** Webserver may be slow to respond during heavy pipeline execution; liveness probe would cause unnecessary restarts.

---

## 7. Credentials

### 7.1 Dagster Access to PostgreSQL

| Credential | Location | Notes |
|------------|----------|-------|
| Database User | `dagster` | Hardcoded in Helm values |
| Database Password | `postgres-secrets` SealedSecret | Encrypted in Git |
| Database Name | `dagster` | Hardcoded in Helm values |

**PostgreSQL User Permissions:**

```sql
-- Create database
CREATE DATABASE dagster;

-- Create user (done via sealed secret)
CREATE USER dagster WITH PASSWORD '<from-secret>';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE dagster TO dagster;
\c dagster
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dagster;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dagster;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dagster;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO dagster;
```

### 7.2 Creating/Updating Secrets

**Full Procedure:**

```bash
# 1. Create plain secret (local only, never commit)
kubectl create secret generic postgres-secrets \
  --from-literal=postgresql-password='<strong-password>' \
  --namespace dagster \
  --dry-run=client -o yaml > /tmp/secret.yaml

# 2. Encrypt with Sealed Secrets controller
kubeseal -o yaml < /tmp/secret.yaml > overlays/prod/sealed-secret.yaml

# 3. Clean up plain secret
rm /tmp/secret.yaml

# 4. Commit sealed secret to Git
git add overlays/prod/sealed-secret.yaml
git commit -m "chore: update PostgreSQL credentials"

# 5. Apply to cluster
kubectl apply -k overlays/prod

# 6. Restart Dagster pods to pick up new credentials
kubectl rollout restart deployment -n dagster dagster-dagster-webserver
kubectl rollout restart deployment -n dagster dagster-dagster-daemon
```

**Password Rotation Schedule:**
- Development: Ad-hoc (when compromised)
- Production: Quarterly (or when staff changes)

---

## 8. Data Management

### 8.1 Persistent Data Locations

Dagster instance is **stateless** - all data stored externally.

| Data Type | Location | Persistence Layer | Business Value |
|-----------|----------|-------------------|----------------|
| Run History | PostgreSQL (`dagster` DB) | Longhorn PVC (3x replication) | High - debugging, audit trail |
| Compute Logs | PostgreSQL (`dagster` DB) | Longhorn PVC (3x replication) | Medium - troubleshooting |
| Asset Metadata | PostgreSQL (`dagster` DB) | Longhorn PVC (3x replication) | High - data lineage |
| Schedules/Sensors | PostgreSQL (`dagster` DB) | Longhorn PVC (3x replication) | Critical - pipeline execution |
| Raw Market Data | MinIO (LXC) | ZFS 3-way mirror | **Critical - irreplaceable** |
| Transformed Data | PostgreSQL (`postgres` DB) | Longhorn PVC (3x replication) | High - analytics input |

**Dagster Tables in PostgreSQL:**
- `runs` - Execution history
- `event_logs` - Structured event stream  
- `asset_keys` - Asset catalog
- `schedules` - Schedule definitions
- `jobs` - Job definitions

**Backup Priority:**
1. **Critical:** MinIO raw data (historical market data - CANNOT be re-extracted)
2. **High:** `postgres` database (transformed data - business value)
3. **Medium:** `dagster` database (run history - debugging value)

### 8.2 Backup Procedure

**PostgreSQL Backups:**

```bash
# Dump dagster database
kubectl exec -n database postgresql-0 -- \
  pg_dump -U postgres dagster > dagster-backup-$(date +%Y%m%d).sql

# Dump all databases (includes dagster + postgres)
kubectl exec -n database postgresql-0 -- \
  pg_dumpall -U postgres > all-databases-backup-$(date +%Y%m%d).sql

# Compressed dump (faster restore)
kubectl exec -n database postgresql-0 -- \
  pg_dump -U postgres -Fc dagster > dagster-backup-$(date +%Y%m%d).dump
```

**Longhorn Volume Snapshots:**

```bash
# Via kubectl
kubectl apply -f - <<EOF
apiVersion: longhorn.io/v1beta2
kind: VolumeSnapshot
metadata:
  name: dagster-db-snapshot-$(date +%Y%m%d)
  namespace: longhorn-system
spec:
  volume: pvc-<pvc-uuid>
EOF
```

**MinIO Historical Data Backup:**

```bash
# Install mc (MinIO client)
# Download from: https://min.io/docs/minio/linux/reference/minio-mc.html

# Configure MinIO alias
mc alias set minio-local http://minio.lxc.local:9000 <access-key> <secret-key>

# Mirror to external storage
mc mirror minio-local/crypto-raw-data /external-hdd/crypto-backups/

# Verify backup
mc ls --recursive /external-hdd/crypto-backups/ | wc -l
```

**Backup Schedule (Recommended):**
- **Weekly:** Logical backup (pg_dump) of `dagster` database
- **Weekly:** Longhorn snapshot of PostgreSQL PVC
- **Daily:** MinIO mirror to external HDD (historical data is irreplaceable)
- **Monthly:** Full backup copied to offsite storage

**Retention Policy:**
- Daily MinIO backups: Keep all (historical data cannot be regenerated)
- Weekly PostgreSQL dumps: Keep 4 weeks
- Weekly Longhorn snapshots: Keep 4 weeks
- Monthly offsite backups: Keep 12 months

**Note:** Dagster compute logs older than 7 days can be safely deleted to reduce database size.

### 8.3 Restore Procedure

**PostgreSQL Restore:**

```bash
# Restore dagster database from dump
kubectl exec -i -n database postgresql-0 -- \
  psql -U postgres dagster < dagster-backup-20251214.sql

# Restore all databases
kubectl exec -i -n database postgresql-0 -- \
  psql -U postgres < all-databases-backup-20251214.sql

# Restore compressed dump
kubectl exec -i -n database postgresql-0 -- \
  pg_restore -U postgres -d dagster < dagster-backup-20251214.dump
```

**Longhorn Volume Restore:**

1. Navigate to Longhorn UI: `http://longhorn.homelab.lan`
2. Select volume → Snapshots tab
3. Choose snapshot → Restore
4. Restart PostgreSQL pod to pick up restored volume

**MinIO Data Restore:**

```bash
# Restore from external storage
mc mirror /external-hdd/crypto-backups/ minio-local/crypto-raw-data/

# Verify restoration
mc ls --recursive minio-local/crypto-raw-data/ | wc -l
```

---

## 9. Troubleshooting

### 9.1 Common Issues

#### Issue: Application not accessible via URL

**Symptoms:** Browser shows connection refused or 404

**Diagnosis:**
```bash
# Layer-by-layer check
1. kubectl get pods -n dagster                      # Pods running?
2. kubectl get svc -n dagster                       # Service exists?
3. kubectl get ingressroute -n dagster              # IngressRoute created?
4. kubectl logs -n traefik -l app.kubernetes.io/name=traefik  # Traefik errors?
5. kubectl port-forward svc/dagster-dagster-webserver 8080:80 -n dagster  # Direct test
```

**Common Causes:**
- Pods not running (check logs)
- Service ClusterIP not assigned
- IngressRoute service name mismatch (see below)
- Traefik not routing to correct backend

#### Issue: IngressRoute Service Name Mismatch

**Symptoms:**
- `curl http://dagster.homelab.lan` returns 404
- `kubectl get ingressroute -n dagster` shows IngressRoute exists
- Traefik logs show no errors

**Root Cause:**
Helm chart creates service named `dagster-dagster-webserver` (following `{releaseName}-{componentName}` convention), but IngressRoute referenced incorrect name.

**Diagnosis:**
```bash
# Find actual service name
kubectl get svc -n dagster

# Expected output:
# NAME                        TYPE        CLUSTER-IP      PORT(S)
# dagster-dagster-webserver   ClusterIP   10.x.x.x        80/TCP
```

**Solution:**
```yaml
# ingressroute.yaml - CORRECT
spec:
  routes:
    - match: Host(`dagster.homelab.lan`)
      services:
        - name: dagster-dagster-webserver  # ✅ Full Helm-generated name
          port: 80

# INCORRECT (will cause 404)
spec:
  routes:
    - match: Host(`dagster.homelab.lan`)
      services:
        - name: dagster  # ❌ Shortened name doesn't exist
          port: 80
```

**Prevention:**
Always check service names after Helm deployment:
```bash
kubectl get svc -n dagster | grep dagster
```

#### Issue: Pod in CrashLoopBackOff

**Symptoms:** Pod repeatedly restarting

**Diagnosis:**
```bash
# Check logs from previous crash
kubectl logs -n dagster <pod-name> --previous

# Check events for OOMKilled or Image pull errors
kubectl describe pod -n dagster <pod-name>

# Verify secrets are mounted
kubectl get secret -n dagster postgres-secrets
kubectl describe pod -n dagster <pod-name> | grep -A 5 "Mounts:"
```

**Common Causes:**
- Missing `postgres-secrets` secret
- PostgreSQL connection refused (database not running)
- OOMKilled due to insufficient memory limits
- Image pull failure (check registry credentials)

#### Issue: Database Connection Failed

**Symptoms:** Application logs show `psycopg2.OperationalError: could not connect to server`

**Diagnosis:**
```bash
# 1. Verify PostgreSQL pod running
kubectl get pods -n database

# 2. Check PostgreSQL service exists
kubectl get svc -n database postgresql

# 3. Test connectivity from Dagster pod
kubectl exec -n dagster <dagster-pod> -- \
  nc -zv postgresql.database.svc.cluster.local 5432

# 4. Verify credentials match
kubectl get secret -n dagster postgres-secrets -o yaml | grep password | base64 -d
kubectl exec -n database postgresql-0 -- \
  psql -U postgres -c "SELECT usename FROM pg_user WHERE usename='dagster';"
```

**Common Causes:**
- PostgreSQL pod not running (check `kubectl get pods -n database`)
- Wrong database name (should be `dagster`)
- Wrong username or password (check sealed secret)
- PostgreSQL not accepting connections (check `pg_hba.conf`)

#### Issue: Code Location Not Loading

**Symptoms:** Dagster UI shows "Code location `trading-data` unavailable"

**Diagnosis:**
```bash
# 1. Check if code location pod running
kubectl get pods -n dagster -l component=user-code

# 2. Check code location service exists
kubectl get svc -n dagster | grep trading-data

# 3. Test gRPC connectivity
kubectl exec -n dagster <webserver-pod> -- \
  nc -zv trading-data.dagster.svc.cluster.local 3030

# 4. Check webserver logs for gRPC errors
kubectl logs -n dagster -l component=webserver | grep gRPC
```

**Common Causes:**
- Code location pod not running
- Service name mismatch in `workspace.servers` configuration
- gRPC port 3030 not exposed in code location service
- Code location failing to start (check pod logs)

### 9.2 Useful Commands

**Pod Management:**
```bash
# View all Dagster pods
kubectl get pods -n dagster

# Follow webserver logs
kubectl logs -n dagster -l component=webserver -f

# Follow daemon logs (schedule execution)
kubectl logs -n dagster -l component=daemon -f

# Exec into webserver pod
kubectl exec -it -n dagster <webserver-pod> -- /bin/bash

# Restart deployments
kubectl rollout restart deployment -n dagster dagster-dagster-webserver
kubectl rollout restart deployment -n dagster dagster-dagster-daemon
```

**Debugging:**
```bash
# Check recent events
kubectl get events -n dagster --sort-by='.lastTimestamp' | head -20

# Describe deployment
kubectl describe deployment -n dagster dagster-dagster-webserver

# Check resource usage
kubectl top pods -n dagster

# Port forward to UI (bypass ingress)
kubectl port-forward -n dagster svc/dagster-dagster-webserver 3000:80
```

**Database Checks:**
```bash
# Connect to PostgreSQL
kubectl exec -it -n database postgresql-0 -- psql -U postgres

# Check dagster database size
kubectl exec -n database postgresql-0 -- \
  psql -U postgres -c "SELECT pg_size_pretty(pg_database_size('dagster'));"

# Count runs
kubectl exec -n database postgresql-0 -- \
  psql -U postgres dagster -c "SELECT COUNT(*) FROM runs;"
```

### 9.3 Job Monitoring

**Via Dagster Web UI:**

Navigate to `http://dagster.homelab.lan`

| Page | Purpose | Key Metrics |
|------|---------|-------------|
| **Runs** | View all job executions | Success rate, duration, failures |
| **Assets** | Check data freshness | Asset staleness, last materialization time |
| **Schedules** | Monitor schedule status | Active schedules, next run time |
| **Sensors** | Monitor sensor status | Last tick, cursor position |
| **Launchpad** | Manually trigger jobs | Test job execution |

**Key Metrics to Monitor:**

| Metric | Location | Healthy Range | Action If Breached |
|--------|----------|---------------|-------------------|
| Run Success Rate | Runs > Overview | >95% | Investigate recent failures |
| Asset Staleness | Assets > Status | No red badges | Check upstream dependencies |
| Execution Duration | Runs > Timeline | <5min for extract jobs | Optimize code or increase resources |
| Schedule Status | Schedules > Overview | All "Running" | Check daemon pod logs |

**Via kubectl (when UI unavailable):**

```bash
# Check if control plane is running
kubectl get pods -n dagster -l component=webserver
kubectl get pods -n dagster -l component=daemon

# View daemon logs (schedule/sensor execution)
kubectl logs -n dagster -l component=daemon --tail=100

# View webserver logs (API requests)
kubectl logs -n dagster -l component=webserver --tail=100

# Check code location health
kubectl get pods -n dagster -l component=user-code
```

**Troubleshooting Job Failures:**

```bash
# Find failed runs (requires exec into webserver pod)
kubectl exec -n dagster <webserver-pod> -- \
  dagster run list --status FAILURE --limit 10

# View logs for specific run (via UI is easier)
# Runs > [select failed run] > Logs tab > expand compute logs
```

**Alerting (Future):**

For production deployments, implement:
- **Dagster Sensors** - Detect asset staleness, job failures
- **Slack Integration** - Immediate notification of critical failures
- **Prometheus Metrics** - Export Dagster metrics for Grafana dashboards
- **PagerDuty** - Escalate critical alerts to on-call engineer

---

## 10. Maintenance

### 10.1 Updating the Application

**Helm Chart Version Update:**

```bash
# 1. Update version in base/kustomization.yaml
#    Change: version: 1.12.6 → version: 1.13.0

# 2. Review Helm chart changelog
#    Visit: https://github.com/dagster-io/helm/releases

# 3. Apply update
kubectl apply -k overlays/prod

# 4. Monitor rollout
kubectl rollout status deployment -n dagster dagster-dagster-webserver
kubectl rollout status deployment -n dagster dagster-dagster-daemon

# 5. Verify health
kubectl get pods -n dagster
curl http://dagster.homelab.lan/dagit_info
```

**Force Restart (Same Image):**

```bash
# Restart all Dagster pods
kubectl rollout restart deployment -n dagster dagster-dagster-webserver
kubectl rollout restart deployment -n dagster dagster-dagster-daemon

# Restart specific code location
kubectl rollout restart deployment -n dagster trading-data
```

### 10.2 Scaling

**Horizontal Scaling (High Availability):**

```bash
# Edit base/kustomization.yaml
# Under dagster-webserver:
#   replicaCount: 3

# Apply changes
kubectl apply -k overlays/prod

# Verify scaling
kubectl get pods -n dagster -l component=webserver
```

**Vertical Scaling (Resource Limits):**

```bash
# Edit base/kustomization.yaml
# Under dagster-webserver.resources:
#   limits:
#     cpu: 1000m
#     memory: 1Gi

# Apply changes
kubectl apply -k overlays/prod

# Monitor resource usage
kubectl top pods -n dagster
```

**Daemon Scaling:**

⚠️ **Never scale daemon >1 replica** - Only one daemon should run (leader election prevents multiple schedule triggers, but wastes resources).

### 10.3 Maintenance Window Checklist

**Pre-Maintenance:**
- [ ] Notify users of scheduled downtime (if applicable)
- [ ] Create backup: `kubectl exec -n database postgresql-0 -- pg_dump -U postgres dagster > backup.sql`
- [ ] Create Longhorn snapshot via UI
- [ ] Verify backup integrity: `psql < backup.sql` (test restore)

**During Maintenance:**
- [ ] Apply changes: `kubectl apply -k overlays/prod`
- [ ] Monitor rollout: `kubectl rollout status deployment -n dagster`
- [ ] Check pod health: `kubectl get pods -n dagster`

**Post-Maintenance:**
- [ ] Verify UI accessible: `curl http://dagster.homelab.lan/dagit_info`
- [ ] Test job execution: Trigger test job via Launchpad
- [ ] Monitor logs for errors: `kubectl logs -n dagster -l app=dagster --tail=100`
- [ ] Verify PostgreSQL connectivity: Check run metadata appears in UI
- [ ] Update changelog: Document changes in [Section 12](#12-changelog)

**Rollback Procedure (If Issues):**

```bash
# 1. Revert to previous Helm chart version
#    Edit base/kustomization.yaml: version: 1.13.0 → version: 1.12.6

# 2. Apply previous version
kubectl apply -k overlays/prod

# 3. Restore database if corrupted
kubectl exec -i -n database postgresql-0 -- \
  psql -U postgres dagster < backup.sql

# 4. Verify rollback successful
kubectl get pods -n dagster
curl http://dagster.homelab.lan/dagit_info
```

---

## 11. References

**Official Documentation:**
- [Dagster Docs](https://docs.dagster.io/)
- [Dagster Helm Chart](https://artifacthub.io/packages/helm/dagster/dagster)
- [Dagster GitHub](https://github.com/dagster-io/dagster)

**Container Images:**
- [Dagster on Docker Hub](https://hub.docker.com/u/dagster)

**Community Resources:**
- [Dagster Slack](https://dagster.slack.com)
- [Dagster Community Discussions](https://github.com/dagster-io/dagster/discussions)

**Related Infrastructure:**
- [PostgreSQL Bitnami Helm Chart](https://github.com/bitnami/charts/tree/main/bitnami/postgresql)
- [Sealed Secrets Controller](https://github.com/bitnami-labs/sealed-secrets)
- [Traefik Kubernetes Ingress](https://doc.traefik.io/traefik/routing/providers/kubernetes-crd/)

**Internal Documentation:**
- [PostgreSQL Deployment Documentation](../postgresql/DEPLOYMENT.md) - Shared database configuration
- [MinIO Deployment Documentation](../minio/DEPLOYMENT.md) - Object storage setup
- [Kubernetes Ingress Setup](../ingress/UNIFIED_INGRESS.md) - MetalLB + Traefik configuration

---

## 12. Changelog

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2025-12-14 | 1.0.0 | <ul><li>Production deployment completed</li><li>Status updated to Running</li><li>Added IngressRoute service name troubleshooting</li><li>Removed incorrect postgres user secret reference</li></ul> | Lior Gefen |
| 2025-12-12 | 0.9.0 | <ul><li>Initial deployment to homelab</li><li>Documented architecture and design decisions</li><li>Created sealed secrets for PostgreSQL</li></ul> | Lior Gefen |

---

**Document Maintenance:**
- This document should be updated whenever:
  - Dagster version is upgraded
  - Configuration changes are made
  - New troubleshooting cases are discovered
  - Backup/restore procedures change
  - Migration paths are executed

- Review cycle: Quarterly (or after major infrastructure changes)
- Owner: Lior Gefen
- Last Reviewed: 2025-12-14
