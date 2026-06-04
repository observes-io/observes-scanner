[![License: Internal Use Only](https://img.shields.io/badge/license-Internal%20Use%20Only-red)](LICENSE)
[![Release](https://img.shields.io/github/v/release/observes-io/observes-scanner?label=release)](https://github.com/observes-io/observes-scanner/releases)
[![Snyk Result](https://img.shields.io/badge/snyk%20scan-result-blue)](https://snyk.io/test/github/observes-io/observes-scanner)
[![Security Policy](https://img.shields.io/badge/security-policy-blue)](SECURITY.md)
[![SBOM](https://img.shields.io/badge/SBOM-published-green)](https://github.com/observes-io/supply-chain/tree/main/observes-scanner)

> **License Summary:** Free for internal organisational use (including production) by your own employees and contractors, solely for your organisation's benefit.  
> Not for embedding, bundling, resale, hosting, offering as a service, or any paid commercial use without a license.  
> See [Polyform Internal Use License](https://polyformproject.org/licenses/internal-use/) for full terms.

# Observes Scanner

[Observes Scanner](https://github.com/observes-io/observes-scanner) is a cross-platform CLI tool for scanning Azure DevOps organizations and its output is parsed by the [Observes UI](https://github.com/observes-io/observes-ui). 

The scanner shapes the data to highlight relationships between CI/CD resources (repositories, agent pools, credentials) and pipelines (runs and and preview runs), along with an analysis of projects stats (repos, commiters, pull requests).

## Features

- Collects organisation and project statistics
- Discovers tasks, pipelines, and CI/CD resources (agent pools, service connections, variable groups, secure files, repositories)
- Audits pipeline permissions and resources' protection state and their use
- Retrieves commit and committer data for projects and repositories

### Pre-Requisites

> You'll be able to perform a full scan of the Azure DevOps organisation or select target projects. However, the scan is limited to the access granted to the identity performing the scan.

1. **An identity with read access** to the targeted Azure DevOps projects (service connections, variable groups, secure files, repositories, pools, repositories). This can be a user account, a service principal, or a managed identity.
2. **Authentication credentials** - one of the following:
   - **PAT token** (read-only) on *Agent Pools*, *Analytics*, *Build*, *Code*, *Graph*, *Project and Team*, *Secure Files*, *Service Connections*, *Task Groups*, *Variable Groups*. *Environments* and *Token Admin* can only be read and manage, but this is optional.
   - **Service principal** (Microsoft Entra app registration) with the identity [added to Azure DevOps](https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/service-principal-managed-identity)
   - **Managed identity** (system- or user-assigned) with the identity [added to Azure DevOps](https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/service-principal-managed-identity)
3. **Ability to run the scanner**: You can run the observes-scanner locally, on Azure compute, or through a pipeline. We provide two example pipelines, one using the extension (recommended) and the other using the standalone scanner.

> **Note:** We recommend using the Observes extension, as it comes prepackaged and enables you to manage scanning and results entirely within the Azure DevOps interface. If you want to install the extension in your ADO organisation you may require approvals for this - [Help me explain this extension](./extension.md)

> **Note:** If you want to see environments, your PAT (or identity) must also have **Environment (Read & Manage)**. The same applies to if you want to see PAT Tokens associated with users. 
> We only require Read, but this is as much least privilege as it currently gets :(

> **Tip:** Microsoft recommends migrating from PATs to Entra ID-based authentication (service principals or managed identities) for automation workloads. Entra tokens are short-lived (~1 hour), automatically rotated, and provide better audit trails.

## Guide: Onboarding Observes Scanner + UI

There are two ways to implement Observes for your ADO organisations:

1. Extension (recommended): Installing the extension makes the scanner and the UI available from the ADO interface
2. Standalone (locally or in the pipeline): Run the standalone scanner and upload the results into the [Observes app](app.observes.io) in the browser


### Guide 1: Leverage the Observes Extension

Installing the Observes extension will make available in your organisation the following resources:

- Service Connection type: Create a service connection of type Observes
- Pipeline Task: Directly use the pre-configured pipeline task in your pipeline
- Hub component: Navigate to the side bar ADO hub component and upload the scan results

To onboard:

1. Create a Service Connection of Observes Azure DevOps Scanner `my-observes-service-connection` with the appropriate PAT
2. Configure and run the pipeline [runme-extension.yml](./examples/runme-extension.yml) with the created service connection. Results will be available in the artefats of the pipeline run
3. Navigate to the Azure DevOps hub component (Azure DevOps interface -> Select any project -> Side bar Observes) - `https://dev.azure.com/ORG/PROJECT/_apps/hub/Observesio.observes.observes-hub` and upload the scan results

### Guide 2: Run Standalone

If the extension is not an option for you at the moment, running the scanner locally and leveraging the app hosted in [app.observes.io](app.observes.io) is an alternative.

1. Scan your ADO organisation following the [locally by installing](#install) the observes-scanner or by configuring and running the pipeline [runme-standalone.yml](./examples/runme-standalone.yml) with the appropriate credentials
2. Navigate to [app.observes.io](app.observes.io) and upload the scan results there

> **Curious about how it feels but don't really want to install an extension nor run the scanner against your organisation?** Head over to [app.observes.io](app.observes.io), download the sample scan results and have a play.

#### Project Structure

- `azuredevops.py`: Core logic for Azure DevOps API integration and enrichment
- `scan.py`: Orchestration and entry point for scanning operations
- `requirements.txt`, `Pipfile`: Python dependencies
- `runme-standalone.yml`: Example pipeline to run the standalone scanner
- `runme-extension.yml`: Example pipeline to run the scanner with the extension installed

#### Install

- Download the appropriate executable for your platform from GitHub Releases, or install the extension from the Azure DevOps Marketplace.
- If running from source:
  ```pwsh
  git clone https://github.com/observes-io/observes-scanner.git
  cd observes-scanner
  pip install -r requirements.txt
  ```

#### Authentication

The scanner supports multiple authentication methods via `--auth-mode`:

| Mode | Flag | Description |
|---|---|---|
| **Default** (recommended) | `--auth-mode default` | Auto-detects credentials via `DefaultAzureCredential`. Tries env vars, workload identity, managed identity, and Azure CLI session in order. Best for pipelines using `AzureCLI@3` with a service connection. |
| **PAT** | `--auth-mode pat -p <TOKEN>` | Personal Access Token (Basic auth). Can also be set via `AZURE_DEVOPS_PAT` env var. Best for local/quick testing. |
| **Service Principal** | `--auth-mode service-principal` | Microsoft Entra app registration (Bearer token). Requires `--tenant-id`, `--client-id`, and `--client-secret` or `--client-certificate-path`. |
| **Managed Identity** | `--auth-mode managed-identity` | Azure-managed identity (Bearer token). Requires running on Azure compute. Use `--client-id` for user-assigned identities. |

The default mode is `default`. When you run `python scan.py -o my_org -j my_scan` without specifying `--auth-mode` or `-p`, the scanner automatically tries to authenticate using `DefaultAzureCredential`, which checks the following sources in order:

1. **Environment variables** (`AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` / `AZURE_CLIENT_CERTIFICATE_PATH`)
2. **Workload identity** (AKS workload identity webhook)
3. **Managed identity** (system- or user-assigned, when running on Azure compute)
4. **Azure CLI session** (when running inside an `AzureCLI@3` pipeline task or after `az login` locally)

This means no explicit credentials are needed when running inside an `AzureCLI@3` pipeline task with a service connection - the scanner picks up the active session automatically.

For PAT authentication specifically:

- As a CLI argument: `-p <pat-token>` or `--pat-token <pat-token>`
- As an environment variable: `AZURE_DEVOPS_PAT`

If using `--auth-mode pat` and neither is provided, the tool will exit with an error.

> **Note:** Service principal and managed identity auth require `pip install azure-identity`.


#### CLI Usage

All configuration and options must be provided as CLI arguments. The following options are available:

```
-o, --organization           Azure DevOps organization name (required)
-j, --job-id                 Job ID for this scan (required)
-p, --pat-token              Azure DevOps Personal Access Token (optional if AZURE_DEVOPS_PAT is set)
    --auth-mode              Authentication method: pat, service-principal, managed-identity, default (default: default)
    --tenant-id              Microsoft Entra tenant ID (for service-principal). Env: AZURE_TENANT_ID
    --client-id              Application/client ID (for service-principal or user-assigned managed-identity). Env: AZURE_CLIENT_ID
    --client-secret          Client secret (for service-principal). Env: AZURE_CLIENT_SECRET
    --client-certificate-path  Path to PEM/PFX certificate (for service-principal, more secure than client secret)
-r, --results-dir            Directory to save scan results (default: current working directory)
    --projects               Optional comma separated list of project names or IDs to filter scan
    --top-branches-to-scan   Number of default plus top branches to scan for each repository. -1 for all branches, 0 for default branch only, >= X for default and X top branches (default: 5)
    --resolve-identities     Enable identity resolution for service connections, variable groups, and secure files (requires laughing-lamp package)
    --skip-feeds             Skip artifact feeds scanning for faster scans
    --skip-builds            Skip builds scanning for faster scans
    --skip-committer-stats   Skip committer stats calculation for faster scans
    --skip-users             Skip users, RBAC, and PAT token discovery
```

Example usage with PAT:

```pwsh
python scan.py \
  --organization <organization> \
  --job-id <job-id> \
  --pat-token <pat-token> \
  --results-dir <results-dir> \
  --projects <project1,project2> \
  --top-branches-to-scan 5 \
  --skip-feeds \
  --skip-committer-stats \
  --skip-builds
```

Example usage with default credentials (recommended for pipelines):

```pwsh
# Inside an AzureCLI@3 task - no credentials needed
python scan.py -o <organization> -j <job-id>
```

Example usage with service principal:

```pwsh
python scan.py \
  --organization <organization> \
  --job-id <job-id> \
  --auth-mode service-principal \
  --tenant-id <tenant-id> \
  --client-id <client-id> \
  --client-secret <client-secret>
```

Example usage with managed identity (from Azure compute):

```pwsh
python scan.py \
  --organization <organization> \
  --job-id <job-id> \
  --auth-mode managed-identity \
  --client-id <managed-identity-client-id>
```

Alternatively, credentials can be set via environment variables:

```pwsh
# PAT
$env:AZURE_DEVOPS_PAT="your-pat-token"
python scan.py -o <organization> -j <job-id> --auth-mode pat

# Service principal
$env:AZURE_TENANT_ID="your-tenant-id"
$env:AZURE_CLIENT_ID="your-client-id"
$env:AZURE_CLIENT_SECRET="your-client-secret"
python scan.py -o <organization> -j <job-id> --auth-mode service-principal
```

The tool queries Azure DevOps and returns results as a JSON file. All sensitive data (tokens, secrets) must be stored securely and never hardcoded.

### Required Permissions

The identity performing the scan (PAT, service principal, or managed identity) must have read access to the targeted resources. For PATs specifically, the following scopes are required:

- **Agent Pools (Read):** Access build agent pool data for pipeline and resource inventory.
- **Analytics (Read):** Retrieve language metrics and analytics data for projects and repositories.
- **Build (Read):** Query build definitions, pipeline runs, build results, and dry runs (preview yaml).
- **Code (Read):** Access repositories, commits, branches, pull requests, and committer information.
- **Environment (Read & Manage):** Access environments data. (We only read data and do not modify it, it is not possible to set read-only permission for environments). Optional.
- **Graph (Read):** Access users, groups, and service principals for identity and permissions mapping.
- **Project and Team (Read):** List and query projects, teams, and related metadata.
- **Secure Files (Read):** Access secure files used in pipelines and releases. We are unable to read the contents.
- **Service Connections (Read):** List and query service endpoints and connections for deployments.
- **Task Groups (Read):** Access shared task groups used in build and release pipelines.
- **Variable Groups (Read):** Access shared variable groups for pipeline configuration. We are unable to read the value of secure variables.
- **Token Admin (Read & Revoke):** List user PAT tokens. Optional.

> You can run the scanner without all these permissions for limited results (i.e.: if access to service connections is not granted, information about service connections will be limited).

## Security Disclosure

If you discover a vulnerability or have a security concern, please contact us directly at **security@observes.io**. We request that you do not publicly disclose security issues until we have had a chance to investigate and address them.

For more information, see our [SECURITY.md](./SECURITY.md) file.

## Troubleshooting

- Ensure your PAT has sufficient permissions for the organization and projects you want to target.
- Review logs and output for API rate limits or connectivity issues.

## Contributing

Under the [Polyform Internal Use License](https://polyformproject.org/licenses/internal-use/), we do not accept code contributions (e.g., pull requests).
We do welcome **feature requests** and **bug reports** via the [Issues](../../issues) section.

## License

This project is licensed under the [Polyform Internal Use License](https://polyformproject.org/licenses/internal-use/).

**You may:**

- Use this software in production **within your own organisation**, including for scanning your own environment.
- Modify it internally for your own use.

**You may not:**

- Sell, resell, redistribute, embed, bundle, host, or offer the software as a service.
- Use it as part of a paid product, service, or consulting engagement without a commercial license.

*“Internal organisational use” means use by employees and individual contractors of your organisation, solely for the benefit of your organisation, and not for any third party.*

## Commercial Licensing

If you wish to use this software for paid commercial purposes or include it in a product or service offered to third parties, please contact us at **info@observes.io** for commercial licensing options.

## Supply Chain Security

We are committed to supply chain security and transparency. A Software Bill of Materials (SBOM) is published for each release.

- [View the SBOM for this project](https://github.com/observes-io/supply-chain)

---

### Resources

- [Observes.io Documentation Home](https://observes.io/docshome/)
- [Scanner Source Code GitHub Repository](https://github.com/observes-io/observes-scanner)
- [UI Source Code GitHub Repository](https://github.com/observes-io/observes-ui)
- [Observes Azure DevOps Marketplace](https://marketplace.visualstudio.com/items?itemName=Observesio.observes)
- [App https://app.observes.io/](https://app.observes.io/) or your own self-hosted version

---

For questions, support, or feature requests, open an issue or contact us at **info@observes.io**.
