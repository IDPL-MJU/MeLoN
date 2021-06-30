# Container Vertical Elasticity for Efficient Big Data Processing

## This is a source code repository for Adaptive Resource Management Scheme in Container-based Cloud Environments
Our adaptive resource management scheme can periodically monitor the resource usage patterns of running containers and dynamically adjust (scale up/down) allocated computing resources by leveraging the vertical elasticity of Docker. The vertical elasticity means that we can dynamically increase or decrease computing resource allocations (e.g., CPU time, cores, memory, and network bandwidth) of a running container. The Adaptive Resource Controller periodically adjusts CPU and Memory limits of running containers if necessary, based on the monitored resource usages and fine-grained resource coordination policies that can reflect the characteristics of big data workloads. 

The adaptive resource management scheme mainly consists of 8 Java classes (https://github.com/IDPL-MJU/MeLoN/tree/master/VerticalElasticity/src/main/java/com/idpl/mju/autoelastic) as followings:
* AutoElastic.java: a main class that periodically allocates/reclaims computing resources to running containers
* GetDockerList.java: getting a list of currently running docker containers
* GetDockerResourceData.java: getting the CPU and memory limits of Docker containers and calculating the current resource usages(%)
* GetHostResourceData.java: checking the host server for free computing resources (that can be allocated additionally to Docker containers)
* ModifyCPULimit.java: adjusting the CPU limits of running containers based on available computing resources
* ModifyMemLimit.java: adjusting the Memory limits of running containers based on available computing resources
* Resource.java: a class that collects static variables to store the data extracted from the above classes
* UpdateResourceLimit.java: a function class that runs inside ModifyCPULimit & ModifyMemLimit classes to reflect the container resource (CPU, memory) limit values to the actual container

### Prerequisites
We are utilizing Docker (https://www.docker.com/get-started) as a container management platform, and Google's cAdvisor (https://github.com/google/cadvisor) for monitoring resource usage patterns (e.g., CPU, Memory) of running Docker containers. For the installation and configuration of Docker and cAdvisor, you can refer to the following web sites.

* Docker: https://docs.docker.com/
* cAdvisor: https://github.com/google/cadvisor/

### Building & Testing
This project can be basically built through Maven (https://maven.apache.org/) and eclipse IDE (https://www.eclipse.org/).
Once building is completed, we can simply run the Adaptive Resource Controller using the java command in the host server where Docker containers are running.

```
java com.idpl.mju.autoelastic.AutoElastic
```

Then, the AutoElastic class periodically adjusts CPU and Memory limits of running containers if necessary, based on the monitored resource usages and fine-grained resource coordination policies. 

The Docker containers that have been used in our big data workloads experiments are also maintained through the Docker Hub (https://hub.docker.com/). We used the Intel's HiBench Suite (https://github.com/Intel-bigdata/HiBench) which is a representative big data benchmark suite that helps us to evaluate different big data frameworks in terms of speed, throughput and system resource utilizations. Therefore, we have maintained Docker images that have already installed Apache Hadoop (https://hadoop.apache.org/) and Apache Spark (https://spark.apache.org/) along with the HiBench Suite as followings.

* https://hub.docker.com/r/cjy2181/hadoop-hibench
* https://hub.docker.com/r/cjy2181/hibench_base



