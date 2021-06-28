# Container Vertical Elasticity for Efficient Big Data Processing

## This is a source code repository for Adaptive Resource Management Scheme in Container-based Cloud Environments
Our adaptive resource management scheme can periodically monitor the resource usage patterns of running containers and dynamically adjust (scale up/down) allocated computing resources by leveraging the vertical elasticity of Docker. The Adaptive Resource Controller periodically adjusts CPU and Memory limits of running containers if necessary, based on the monitored resource usages and pre-defined resource allocation policies. 

### Prerequisites
We are utilizing Docker (https://www.docker.com/get-started) as a container management platform, and Google's cAdvisor (https://github.com/google/cadvisor) for monitoring resource usage patterns (e.g., CPU, Memory) of running Docker containers. For the installation and configuration of Docker and cAdvisor you can refer to the following web sites.

* Docker: https://docs.docker.com/
* cAdvisor: https://github.com/google/cadvisor/

### Installation

