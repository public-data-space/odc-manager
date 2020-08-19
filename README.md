# IDS Open Data Connector Manager

This repository contains the logic of the Open Data Connector's core component. It provides REST endpoints and manages metadata 
about data sources, data assets, brokers and the IDS Information model.

The main repository of the IDS Open Data Connector is: 

ids-open-data-connector

## Requirements
* docker
* docker-compose 3.5 or higher
* maven
* DAPS Certificates

## Building the Component
* run ``mvn clean package``

## Using the Component
Use the docker-compose_build.yml file in the *ids-open-data-connector* repository to boot the component along with the 
other connector components.

