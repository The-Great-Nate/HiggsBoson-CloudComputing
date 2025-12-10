# HZZ - Higgs Boson Data Analysis - _Scaled with Cloud Technology_

<img width="1300" height="477" alt="ATLASXDOCKER" src="https://github.com/user-attachments/assets/dbf279b4-36de-4b93-89c6-e630c2a18423" />

This repository is effectively a fork of the [Jupyter Notebook reproducing the data analysis to rediscover the Higgs boson](https://github.com/atlas-outreach-data-tools/notebooks-collection-opendata/blob/master/13-TeV-examples/uproot_python/HZZAnalysis.ipynb).
The goal of this fork is to enable parallelisation of the data analysis process using cloud technology, allowing for a high-throughput system.

To integrate Cloud Technology in the analysis, the original code was Dockerised (to work with docker). 
Crucial steps in the data analysis were broken down into various containers with work being split by data file to $N$ workers.

## Installation and Dependancies
1. Download _Docker Desktop_ and log into your account (or make one)
2. Clone this Repository: `git clone https://github.com/The-Great-Nate/HiggsBoson-CloudComputing`
3. `cd HiggsBoson-CloudComputing`
4. Create a _Docker Swarm_ with `docker swarm init`
5. Build the docker images with the conveniently written shell script `./docker_stack_build.sh`
6. Deploy the docker stack with `docker stack deploy -c docker-compose.yml <STACK NAME>`

### Extra Notes
- You can delete the stack with `docker stack rm <STACK NAME>` if you want to change any python (`.py`) file in the image directories.
  - Please note: you must rerun `./docker_stack_build` if any changes are made to any file within a service folder.
- You can access the output plot in the created volume (`<STACK NAME>_HZZ-outputs`) with the docker desktop GUI. Terminal access into the volume is also possible. The plot is placed in `/data/figures/` in the created volume
- Increase the number of workers through editing the `worker` service within the `docker-compose.yml` file by changing the number of `replicas: ` under the `deploy` section

<img width="1876" height="1294" alt="Screenshot From 2025-12-05 17-42-04" src="https://github.com/user-attachments/assets/8129d7fe-a025-4feb-b748-8ae36eae7615" />
