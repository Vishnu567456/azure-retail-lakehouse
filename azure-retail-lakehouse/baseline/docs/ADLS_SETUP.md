# Use your own Azure Data Lake Storage Gen2

This is an alternative to the default managed volume, not a prerequisite for the first demo. Use the same Azure region as your workspace where practical. These steps create billable storage resources if you do not already have them; the package does not execute them automatically.

1. In Azure Portal, create or reuse a StorageV2 storage account with hierarchical namespace enabled. Suggested name: `YOUR_STORAGE_ACCOUNT` (must be globally unique; append a suffix if unavailable). For a small demo use an appropriate standard locally redundant configuration.
2. Create a private container named `retail`. Reserve the path `lakehouse-files/` for this project. Do not overlap it with an existing external table or volume location.
3. Create or reuse an Access Connector for Azure Databricks with a system-assigned managed identity. Copy its Azure resource ID.
4. Assign the connector identity **Storage Blob Data Contributor** scoped to this container. You need permission to create Azure role assignments. Storage firewall/private endpoint rules must also allow workspace access; RBAC alone does not override network restrictions.
5. In Databricks Catalog Explorer, create a storage credential using the Azure managed identity and connector resource ID. Name it `retail_credential`. This requires the appropriate Unity Catalog privilege; ask the workspace/metastore administrator if unavailable. Do not place account keys or passwords into notebooks.
6. Create and validate an external location `retail_location` for `abfss://retail@YOUR_STORAGE_ACCOUNT.dfs.core.windows.net/lakehouse-files/` using that credential. Substitute the actual account name.
7. Before running `01_setup`, execute the following as a suitably privileged user, replacing the catalog/prefix if needed:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.vishnu_retail_ops;
CREATE EXTERNAL VOLUME IF NOT EXISTS workspace.vishnu_retail_ops.files
LOCATION 'abfss://retail@YOUR_STORAGE_ACCOUNT.dfs.core.windows.net/lakehouse-files/';
```

The caller needs USE CATALOG, USE SCHEMA, CREATE VOLUME on the schema and CREATE EXTERNAL VOLUME on the external location (or ownership/admin privileges). Grant the pipeline identity READ VOLUME and WRITE VOLUME on this volume along with catalog/schema access.

8. Run `01_setup`. Its `CREATE VOLUME IF NOT EXISTS` preserves the external volume. All notebooks keep using `/Volumes/<catalog>/<prefix>_ops/files` for landing, schemas, and checkpoints.

If you already ran setup with a managed volume, choose a **new prefix**, create its external volume, and rerun all steps under that prefix. Do not drop a working volume to change its type.

This makes the landing/checkpoint files live in your own ADLS container. Bronze/Silver/Gold tables remain managed tables in the catalog's configured storage. Using your ADLS account for table managed storage is a separate catalog/schema administrative configuration, not something this project changes.
