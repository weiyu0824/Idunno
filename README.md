# Idunno
## Overview
Idunno is a simple machine learning platform that can schdule machine learning jobs. 
### What Idunno can do?
1. Detect machines failures
2. Store your dataset on different machine, so that data won't lose
3. Schedule jobs so that each jobs could fairly use resource.


This is the MP2~MP4 for UIUC CS425 course.

## Architecture
<img src="/imgs/architectture.png" alt="Alt text" title="Optional title">

## How to run the project
1. Start the coordinator (hard coded as VM1) as bash run_core.sh and then type join to join the network
2. Start VMs as bash run_core.sh and then type join to join the network
3. From any of the start VMs, you can infer by typing infer [test-data-size] [model] [batch_size]. So infer 100 AlexNet 10 will run inference on 100 images using AlexNet with a batch size of 10. We currently only support ResNet and AlexNet
4. The rest of the commands such as C1, C2, C3, C4, and C5 can be run as C[n] [job name]. So running C2 job1 will return the query statistics of job names "job1"
