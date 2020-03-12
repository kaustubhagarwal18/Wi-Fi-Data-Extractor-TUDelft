# TU Delft Wifi Access point Data extractor(part of - Responsible IOT Data Research)

A python application which extracts data from the [Cisco Prime](https://www.cisco.com/c/en/us/products/cloud-systems-management/prime-infrastructure/index.html) infrastructure of TU Delft and streams the data to [Apache Kafka](https://kafka.apache.org/) to be used by researchers. 
The following queries can be fetched by the producers - 
- Client Stats (provides parameters like Rssi, SNR, data rate , packets received/sent etc for a particular client)
- Client Sessions (provides parameters like ssid,throughput,protocol, vlan etc for the session)
- Client Details (provides paramaeter like mobility, wepstatus, wired/wireless, ssid , vendor , location etc)
- Historical Client Counts

There is a producer program for each query which pushes data to different topics(Daily,Monthly,Vendor) after anonymizing it on a daily , monthly basis. 
Three consumer programs based on day , month and vendor
topic collects
the required data and writes it to a csv file.

## Prerequisites
Docker is required to run the system and can be installed by - 

* [Install Docker](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

## Getting Started
You can clone or download the repository to run it on your local machine for development and testing purposes. The docker file will run the environment and also install
the necessary libraries like -
* [python package installer](https://pip.pypa.io/en/stable/) 
* [flatten-json](https://pypi.org/project/flatten-json/) 
* [kafka-python](https://github.com/dpkp/kafka-python)
* [Curl](https://curl.haxx.se/)
* [Pandas](https://pypi.org/project/pandas/)

## Installing
The following command builds the images and the containers for the Dockerfile-
```
docker build .
```
Execute this command to run the dockerfile
```
docker-compose up
```
The status of the docker containers can be checked by the following command
```
docker ps
```
After this you can enter the folder of choice in queries(Eg - ClientSessions) and run the producer program using - 

```
python producer.py

```
The different consumer programs(eg daily topic) can be run using the following command-

```
python consumer_day.py
```
The consumer will save the csv flies in the same directory. 

## Paging for large amount of data
The data can be queried in a [paged](https://solutionpartner.cisco.com/media/prime-infrastructure/api-reference/szier-m8-106.cisco.com/webacs/api/v3/index9df8.html?id=paging-doc) format like -  
```
- Devices?.full=true&.firstResult=0&.maxResults=4    (First four results(0-3))
- Devices?.full=true&.maxResults=4&.firstResult=4    (four results (4-7))
```

## License
This project is licensed under the [MIT License](https://opensource.org/licenses/MIT)

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).






