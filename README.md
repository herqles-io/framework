# Herqles Framework

The Framework creates and manages Jobs.

# Version 2.0

This version is a compelte rewrite and is not compatible with older versions. 
Please use caution when upgrading.

## Requirements

* Herqles Manager
* RabbitMQ Server(s)
* Python 2.7
    * Not tested with newer python versions
 
## Quick Start Guide

Install HQ-Framework into a python environment

```
pip install hq-framework
```

Install Framework implementations

```
pip install myframework
```

Setup the configuration for the Framework

```yaml
rabbitmq:
  hosts:
    - "10.0.0.1:5672"
    - "10.0.0.2:5672"
    - "10.0.0.3:5672"
  username: "root"
  password: "root"
  virtual_host: "herqles"
sql:
  driver: 'postgres'
  host: '10.0.0.1'
  port: 5432
  database: 'herqles'
  username: 'root'
  password: 'root'
paths:
  logs: '/var/logs/herqles'
  pid: '/var/run/herqles/framework.pid'
  framework_configs: '/etc/herqles/hq-framework/config.d'
```

Setup the configuration for any framework implementations in the framework_configs folder

framework_configs/myframework.yml
```yaml
module: 'my.awesome.framework'
```

Run the Framework

```
hq-framework -c config.yml
```

You now have a fully functional Framework for the Herqles system.
