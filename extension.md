The scanner and UI code, along with SBOMs and licenses are available [here](https://github.com/observes-io/observes). There is an Azure DevOps extension [Observes Scanner](https://placeholder) that makes it easy to set it up. Obtain it from the market place. <!-- @TODO Replace with actual ext -->


## Need to Know 

- **Transparency**
    - We provide Software Bill of Materials (SBOMs) and vulnerability reports for the extension itself, ensuring full transparency into its components and security posture.

- **Project and Access Control**
    - The extension can be scoped to a single project, ensuring that access is limited and targeted. Only read access is required.
    - All actions are bounded to the user and the Personal Access Token (PAT) used in the scan, limiting the blast radius.

- **Data Security**
    - No data leaves the organisation in the self-hosted scan version. Data collection, processing and visualisation remains within your environment.

- **Visibility and Insights**
    - The resulting artefact combines the data in a way designed to help visualising the relationships between credentials and pipelines, allowing you to uncover usage insights.


### Resources

- [Observes.io Documentation Home](https://https://observes.io/docshome/)
- [Scanner Source Code GitHub Repository](https://github.com/observes-io/observes-scanner)
- [UI Source Code GitHub Repository](https://github.com/observes-io/observes-ui)
- [Observes Azure DevOps Marketplace](https://placeholder) <!-- @TODO Replace with actual ext -->
- [App https://app.observes.io/](https://app.observes.io/) or your own self-hosted version
