# MeLoN
MeLoN(Multi-tenant Deep Learning framework on YARN): Distributed Deep Learning meets the Big Data Platform

We have designed and implemented a new data processing framework called MeLoN which aims to effectively support distributed deep learning applications that can show another type of data-intensive workloads in the YARN-based Hadoop ecosystem. MeLoN is developed as one of Hadoop YARN applications so that it can transparently co-host existing deep learning applications with other data processing workflows. MeLoN employs comprehensive techniques that can effectively support multiple deep learning applications in a Hadoop YARN cluster by leveraging fine-grained GPU over-provisioning policy and a high-performance parallel file system for data staging which can improve the overall system throughput and utilization. 

Through our extensive experiments based on the representative deep learning workloads, we demonstrate that MeLoN can make an effective convergence of deep learning frameworks and the big data platform Hadoop. We believe that MeLoN can bring many additional interesting research issues including profiling of expected GPU memory usages of deep learning appli-
cations, supporting more complicated deep learning related jobs based on queuing systems which can ultimately contribute to a new data processing framework in the YARN-based Hadoop ecosystem.
