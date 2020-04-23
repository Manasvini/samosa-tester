# samosa-tester
driver app to test pulsar-client-samosa  
## Introduction  
This repository builds on [pulsar-location-samosa](https://github.com/Manasvini/pulsar-location-samosa) and [indexPerf](https://github.com/Manasvini/indexPerf).  
The test runner is basically designed to spawn a `p` producer threads, `c` consumer threads and `p+c` location manager threads.  
The location manager takes in a route file as input which simulates movement and triggers topic changes based on the index configuration. A sample config file is included in the `bin/` folder.  
Metrics such as publish rate, end to end latency and subscription changes are written to the output json specified in the config file  
## Building  
```shell  
$ cd path/to/samosa-tester  
$ mvn compile  
$ mvn install  
  
## Running the code  
``shell  
$ cd bin  
$ bash run.sh config.yaml  
```
Note that the config file specifies the paths to the route files which are in the following format:   
```csv  
0.0,0.0  
0.1,0.0  
0.2,0.0  
0.2,0.1  
```
