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

> You'll be able to perform a full scan of the Azure DevOps organisation or select target projects. However, the scan is limited to the access granted to the user identity performing the scan.

1. **User or service account with read access** to the targeted Azure DevOps projects (service connections, variable groups, secure files, repositories, pools, repositories)
2. **Read-only PAT token** (linked to the account that performs the scan) on *Agent Pools*, *Analytics*, *Build*,*Code*,*Graph*,*Project and Team*,*Secure Files*,*Service Connections*,*Task Groups* and *Variable Groups*
3. **Ability to run the scanner**: You can run the observes-scanner locally with a PAT or through a pipeline. We give you two example pipelines, one using the extension (recommended) and the other using the standalone observes scanner (if the extension is not available). 

> **Note:** We recommend using the Observes extension, as it comes prepackaged and enables you to manage scanning and results entirely within the Azure DevOps interface. If you want to install the extension in your ADO organisation you may require approvals for this - [Help me explain this extension](./extension.md)

> **Note:** If you want to see environments, your PAT must also have **Environment (Read & Manage)**
> We only require Read, but this is as much least privilege as it currently gets :(

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

The Azure DevOps Personal Access Token (PAT) can be provided in two ways:

- As a CLI argument: `-p <pat-token>` or `--pat-token <pat-token>`
- As an environment variable: `AZURE_DEVOPS_PAT`

If both are provided, the CLI argument takes precedence. If neither is provided, the tool will exit with an error.


#### CLI Usage

All configuration and options must be provided as CLI arguments. The following options are available:

```
-o, --organization           Azure DevOps organization name (required)
-j, --job-id                 Job ID for this scan (required)
-p, --pat-token              Azure DevOps Personal Access Token (optional if AZURE_DEVOPS_PAT is set)
-r, --results-dir            Directory to save scan results (default: current working directory)
    --enable-secrets-scanner Enable secrets scanner (default: disabled)
    --projects               Optional comma separated list of project names or IDs to filter scan
```

Example usage:

```pwsh
./observes-scanner.exe -o <organization> -j <job-id> -p <pat-token> [-r <results-dir>] [--enable-secrets-scanner] [--projects <project1,project2>]
```
Or, if running from source:
```pwsh
python scan.py -o <organization> -j <job-id> -p <pat-token> [-r <results-dir>] [--enable-secrets-scanner] [--projects <project1,project2>]
```

Alternatively, you can set the PAT as an environment variable:

```pwsh
$env:AZURE_DEVOPS_PAT="your-pat-token"
python scan.py -o <organization> -j <job-id>
```

The tool queries Azure DevOps and returns results as a JSON file. All sensitive data (tokens, secrets) must be stored securely and never hardcoded.

### Required PAT Permissions

The Azure DevOps Personal Access Token (PAT) must have the following permissions:

- **Agent Pools (Read):** Access build agent pool data for pipeline and resource inventory.
- **Analytics (Read):** Retrieve language metrics and analytics data for projects and repositories.
- **Build (Read):** Query build definitions, pipeline runs, build results, and dry runs (preview yaml).
- **Code (Read):** Access repositories, commits, branches, pull requests, and committer information.
- **Environment (Read & Manage):** Access environments data. (We only read data and do not modify it, it is not possible to set read-only permission for environments)
- **Graph (Read):** Access users, groups, and service principals for identity and permissions mapping.
- **Project and Team (Read):** List and query projects, teams, and related metadata.
- **Secure Files (Read):** Access secure files used in pipelines and releases. We are unable to read the contents.
- **Service Connections (Read):** List and query service endpoints and connections for deployments.
- **Task Groups (Read):** Access shared task groups used in build and release pipelines.
- **Variable Groups (Read):** Access shared variable groups for pipeline configuration. We are unable to read the value of secure variables.

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
