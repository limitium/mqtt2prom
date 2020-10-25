# Prometheus exporter for MQTT
![ci](https://github.com/limitium/mqtt2prom/workflows/ci/badge.svg)

## Description:

Converts MQTT data from `${topic}/userName/zoneName/sensorName/parameterType value` to prometheus Gauges

`zoneName_sensorName_parameterType{u="userName"} value`

## Usage:

- Create a folder to hold the config (default: "conf/")
- Add config in yaml format to the folder. (See exampleconf/conf.yaml for details)
- Run  ./mqtt_exporter.py
- Profit!

## Config:

Yaml files in the folder config/ is combined and read as config.
See exampleconf/ for examples.

## Python dependencies:

 - paho-mqtt
 - prometheus-client
 - PyYAML
 - yamlreader

## Todo:

Add persistence of metrics on restart
