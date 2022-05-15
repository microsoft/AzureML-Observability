# Project

### The Azure ML Observability solution accelerator provides libraries for model monitoring and data drift analysis.
<br>To install early release version:
1. For Data Collection to ingest data: 

    ```pip install git+https://github.com/microsoft/AzureML-Observability#subdirectory=aml-obs-collector```  
2. For client library: 

    ```pip install git+https://github.com/microsoft/AzureML-Observability#subdirectory=aml-obs-client```

    ```pip install azure-ai-ml==0.0.62653692 --extra-index-url https://azuremlsdktestpypi.azureedge.net/sdk-cli-v2```
## Overview
<br>

![Overview](./media/aml_obs_overview.png)

## Architecture
### The solution is built on top of Azure Data Explorer and Azure ML.
<br>

![Architecture](./media/architecture.png)

## Quick Start
Check out quick start notebooks
<br>[1. Solution provisioning ](./quick_start/0_provision.ipynb)
<br>[2. Monitoring](./quick_start/1_monitoring.ipynb)
<br>[3. Drift](./quick_start/2_drift.ipynb)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
