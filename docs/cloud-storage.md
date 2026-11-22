# Cloud Object Storage

K2I uploads Parquet data files to the object store named by
`iceberg.warehouse_path`. The scheme selects the backend:

| Warehouse path | Backend |
|---|---|
| `s3://bucket/prefix` | Amazon S3 and S3-compatible stores (MinIO, LocalStack) |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://container/prefix` or `abfs://container@account.dfs.core.windows.net/prefix` | Azure Blob Storage |
| anything else | local filesystem |

The catalog commit is a separate hop: a REST, Glue, Hive, or Nessie catalog
records the file, so its credentials are configured separately (see
[Iceberg REST Catalog](./iceberg-rest-catalog.md)). This guide covers only the
object-store hop.

## What Is Actually Tested

The backends are wired end to end, but the level of proof differs. Read this
table before claiming a backend is validated in your environment.

| Backend | Covered by an automated test | Not covered by any test |
|---|---|---|
| S3 / S3-compatible | Adobe S3Mock container round trip: the writer uploads a Parquet file, the catalog-visible URI resolves to the stored object with the expected size, and no object appears at a doubled or missing prefix. Store construction with no explicit keys. | IRSA, EC2 instance profiles, ECS task roles, and environment credentials are not exercised: they need real AWS credentials and are not simulated. |
| GCS | Store construction with no explicit credentials; bucket/prefix unit tests. | No live GCS bucket, no Workload Identity, no ADC exchange. |
| Azure Blob | Store construction with no explicit credentials; container/prefix unit tests; missing-account error. | No live Azure account, no Managed Identity, no Workload Identity token exchange. |

The S3 test is ignored unless Docker is available:

```bash
cargo test -p k2i-core --test integration_tests -- --ignored s3_object_store
```

Everything else below describes how the `object_store` 0.11 SDK is expected to
resolve credentials. It follows the SDK source and its documentation, not a test
run against a cloud account. The orderings were checked against
`object_store` 0.11.2: `src/aws/builder.rs`, `src/gcp/builder.rs` with
`src/gcp/credential.rs`, and `src/azure/builder.rs`. **Store construction does
not authenticate**: a process can start, pass `k2i validate`, and report healthy,
then fail with 401/403 on the first flush. Validate credentials with a real flush
before rollout.

## How Credentials Are Resolved

Explicit K2I configuration always wins. When the credential fields are omitted,
K2I seeds the SDK builder from the backend's standard environment variables
(`AmazonS3Builder::from_env`, `GoogleCloudStorageBuilder::from_env`,
`MicrosoftAzureBuilder::from_env`) and the SDK then falls through to its own
credential providers.

K2I's `K2I_ICEBERG_*` variables (for example
`K2I_ICEBERG_AWS_ACCESS_KEY_ID`) override the TOML file. The native cloud
variables below are read by the SDK, not by K2I's config layer. Both work; the
`K2I_` names are the ones listed in
[Kubernetes deployment](./kubernetes.md#supported-variables).

### S3

Credential resolution order:

1. `iceberg.aws_access_key_id` + `iceberg.aws_secret_access_key` (static keys).
2. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional
   `AWS_SESSION_TOKEN`).
3. Web identity token: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`
   (this is what EKS IRSA injects; optional `AWS_ROLE_SESSION_NAME`).
4. ECS task role: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
5. EC2 instance metadata (IMDSv2 by default; set `AWS_IMDSV1_FALLBACK=true` only
   for old node setups such as kube2iam that cannot serve IMDSv2).

Region and endpoint come from `iceberg.aws_region` and `iceberg.s3_endpoint`
when set, otherwise from `AWS_REGION` / `AWS_DEFAULT_REGION` and
`AWS_ENDPOINT_URL` / `AWS_ENDPOINT`. `AWS_ALLOW_HTTP=true` permits plain HTTP.

Limits worth knowing:

- Shared AWS profiles (`~/.aws/credentials`, `AWS_PROFILE`) and SSO are **not**
  read by `object_store` 0.11. Use environment variables, IRSA, or explicit
  config instead.
- With explicit keys in the TOML/K2I config, native `AWS_*` credential variables
  are not consulted, so a stray `AWS_SESSION_TOKEN` cannot be mixed into a static
  key pair.

For MinIO or another S3-compatible store, set `s3_endpoint`; HTTP endpoints are
allowed automatically:

```toml
[iceberg]
warehouse_path = "s3://warehouse/k2i"
aws_region = "us-east-1"
s3_endpoint = "http://localhost:9000"
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"
```

### GCS

Credential resolution order:

1. `iceberg.gcs_service_account_path` (path to a service-account JSON key).
2. `GOOGLE_SERVICE_ACCOUNT_PATH` / `GOOGLE_SERVICE_ACCOUNT` (path) or
   `GOOGLE_SERVICE_ACCOUNT_KEY` (inline JSON), then
   `GOOGLE_APPLICATION_CREDENTIALS`.
3. The gcloud well-known file
   (`~/.config/gcloud/application_default_credentials.json`).
4. The GCE/GKE metadata server. On GKE, Workload Identity uses this path.

`object_store` 0.11 parses ADC files of type `service_account` and
`authorized_user` only. An `external_account` file (Workload Identity Federation
configured outside GKE) is rejected; use a service-account key file mounted from
a Secret instead.

The bucket comes from `warehouse_path`; `gcs_bucket_name` overrides it while the
in-bucket prefix still comes from the path.

### Azure

Credential resolution order:

1. `iceberg.azure_access_key` (storage account key).
2. `AZURE_STORAGE_ACCOUNT_KEY` / `AZURE_STORAGE_ACCESS_KEY`.
3. Workload Identity: `AZURE_CLIENT_ID` + `AZURE_TENANT_ID` +
   `AZURE_FEDERATED_TOKEN_FILE` (what the AKS workload-identity webhook
   injects). `AZURE_AUTHORITY_HOST` selects a sovereign cloud.
4. Service principal: `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` +
   `AZURE_TENANT_ID`.
5. Azure CLI, only when `AZURE_USE_AZURE_CLI=true`. Not appropriate for pods.
6. Managed Identity through IMDS. `AZURE_CLIENT_ID` selects a user-assigned
   identity; `AZURE_OBJECT_ID` and `AZURE_MSI_RESOURCE_ID` are also accepted.

`azure_storage_account_name` is always required in the K2I config (or
`K2I_ICEBERG_AZURE_STORAGE_ACCOUNT_NAME`); it cannot be derived from the path and
validation fails at startup without it. The container comes from
`warehouse_path`; `azure_container_name` overrides it.

## Kubernetes Setup

### EKS with IRSA

1. Associate an OIDC provider with the cluster (IRSA prerequisite).
2. Create an IAM role whose trust policy is federated to that provider and
   scoped to the namespace and service account:

   ```json
   {
     "Effect": "Allow",
     "Principal": { "Federated": "arn:aws:iam::<account>:oidc-provider/<issuer>" },
     "Action": "sts:AssumeRoleWithWebIdentity",
     "Condition": {
       "StringEquals": { "<issuer>:sub": "system:serviceaccount:<namespace>:<ksa>" }
     }
   }
   ```

3. Allow the writer to upload under the warehouse prefix. The writer calls
   `PutObject`; maintenance tasks, when wired, also list and delete. Grant read,
   list, and delete only if the same identity verifies files or runs cleanup:

   ```json
   {
     "Effect": "Allow",
     "Action": ["s3:PutObject"],
     "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
   }
   ```

4. Annotate the pod's service account:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: k2i
     namespace: k2i
     annotations:
       eks.amazonaws.com/role-arn: arn:aws:iam::<account>:role/k2i-writer
   ```

5. Omit `aws_access_key_id` and `aws_secret_access_key`, and set the region:

   ```toml
   [iceberg]
   warehouse_path = "s3://<bucket>/<prefix>"
   aws_region = "eu-west-1"
   ```

The EKS web identity webhook injects `AWS_ROLE_ARN`,
`AWS_WEB_IDENTITY_TOKEN_FILE`, and `AWS_STS_REGIONAL_ENDPOINTS`; K2I needs no
further configuration. EC2 instance profiles and ECS task roles work the same
way (steps 3 and 5 only) because the SDK falls back to IMDS or the task-role
endpoint.

### GKE with Workload Identity

1. Enable Workload Identity on the cluster and node pool.
2. Create a Google service account, allow the Kubernetes service account to
   impersonate it, and grant bucket access:

   ```bash
   gcloud iam service-accounts add-iam-policy-binding \
     k2i-writer@<project>.iam.gserviceaccount.com \
     --role roles/iam.workloadIdentityUser \
     --member "serviceAccount:<project>.svc.id.goog[<namespace>/<ksa>]"

   gcloud storage buckets add-iam-policy-binding gs://<bucket> \
     --member "serviceAccount:k2i-writer@<project>.iam.gserviceaccount.com" \
     --role roles/storage.objectAdmin
   ```

3. Annotate the Kubernetes service account:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: k2i
     namespace: k2i
     annotations:
       iam.gke.io/gcp-service-account: k2i-writer@<project>.iam.gserviceaccount.com
   ```

4. Point the warehouse at the bucket and omit `gcs_service_account_path`:

   ```toml
   [iceberg]
   warehouse_path = "gs://<bucket>/<prefix>"
   ```

The pod's metadata server returns the federated token; no key file is mounted.
For local development outside GCP, set `GOOGLE_APPLICATION_CREDENTIALS` to a
service-account JSON key, or set `gcs_service_account_path`.

### AKS with Workload Identity or Managed Identity

Workload Identity:

1. Enable the OIDC issuer and Workload Identity on the cluster.
2. Create a user-assigned managed identity, add a federated credential for the
   service account subject `system:serviceaccount:<namespace>:<ksa>`, and grant
   it **Storage Blob Data Contributor** on the storage account or container.
3. Annotate the service account and label the pod so the webhook injects the
   token:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: k2i
     namespace: k2i
     annotations:
       azure.workload.identity/client-id: <client-id>
   ---
   apiVersion: v1
   kind: Pod
   metadata:
     labels:
       azure.workload.identity/use: "true"
   ```

4. Configure the account and warehouse, and omit `azure_access_key`:

   ```toml
   [iceberg]
   warehouse_path = "az://<container>/<prefix>"
   azure_storage_account_name = "<storage-account>"
   ```

Managed Identity without Workload Identity also works: attach a system-assigned
identity to the node pool, grant it **Storage Blob Data Contributor**, and leave
out the credential fields. For a user-assigned identity, set `AZURE_CLIENT_ID`
so K2I selects it through IMDS.

## Local Development

| Backend | Environment |
|---|---|
| S3 / S3-compatible (MinIO, S3Mock) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, `AWS_REGION`, and `s3_endpoint` in config or `AWS_ENDPOINT_URL` |
| GCS | `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`, or `gcloud auth application-default login` |
| Azure | `AZURE_STORAGE_ACCOUNT_KEY`, or `az login` plus `AZURE_USE_AZURE_CLI=true` |

`k2i validate --config ...` checks configuration shape only; it does not contact
the object store or exchange credentials.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `MissingAccessKeyId` or `MissingSecretAccessKey` at startup | Only one of the two S3 keys is set. | Set both, or omit both to use the default chain. |
| S3 `403 AccessDenied` on the first flush, store built fine | IRSA trust policy, bucket policy, or prefix mismatch. | Check the role's trust `sub` condition, then `s3:PutObject` on `<bucket>/<prefix>/*`. |
| S3 `403` with an assumed role in a shell | A stray `AWS_SESSION_TOKEN` or a different profile is active. | Use a clean environment or rely on IRSA in the cluster. |
| S3 requests go to the wrong endpoint | `AWS_ENDPOINT_URL`/`AWS_ENDPOINT` is set in the environment. | Unset it, or set `iceberg.s3_endpoint` explicitly. |
| IMDS connection timeouts before the first request | Running off AWS with no static keys. | Set keys, mount IRSA, or point at MinIO with `s3_endpoint`. |
| GCS `unknown variant external_account` | ADC file is a Workload Identity Federation config, unsupported by `object_store` 0.11. | Mount a `service_account` key and set `gcs_service_account_path`. |
| GCS `401` on GKE | KSA annotation missing, or the GSA binding names the wrong namespace/service account. | Recheck `iam.gke.io/gcp-service-account` and the `svc.id.goog` binding. |
| Azure `403` right after a role assignment | Azure RBAC propagation delay, or wrong container. | Wait a few minutes; verify the container in `warehouse_path`. |
| Azure IMDS timeout | No managed identity attached, or IMDS blocked by network policy. | Attach a managed identity, use Workload Identity, or set `azure_access_key`. |
| Azure picks the wrong identity | Multiple user-assigned identities on the node. | Set `AZURE_CLIENT_ID` to the intended identity. |

## See Also

- [Configuration](./configuration.md) for every storage key and the secret-injection forms.
- [Kubernetes deployment](./kubernetes.md) for the full `K2I_*` environment table.
- [Iceberg REST Catalog](./iceberg-rest-catalog.md) for catalog-side credentials.
- [Troubleshooting](./troubleshooting.md) for ingestion and recovery failures.
