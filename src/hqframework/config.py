import yaml
from schematics.models import Model
from schematics.types import StringType, BaseType, IntType
from schematics.types.compound import DictType, ListType


def parse_config(config_path):
    with open(config_path, "r") as f:
        config = yaml.load(f)
    return config


class BaseConfig(Model):
    rabbitmq = DictType(BaseType(), required=True)
    sql = DictType(BaseType(), required=True)
    paths = DictType(BaseType(), required=True)


class RabbitMQConfig(Model):
    hosts = ListType(StringType(), required=True, min_size=1)
    username = StringType(required=True)
    password = StringType(required=True)
    virtual_host = StringType(default="/")


class SQLConfig(Model):
    driver = StringType(required=True)
    host = StringType(required=True)
    port = IntType(required=True)
    database = StringType(required=True)
    username = StringType(required=True)
    password = StringType(required=True)
    pool_size = IntType(default=20)


class PathConfig(Model):
    logs = StringType(required=True)
    pid = StringType(required=True)
    framework_configs = StringType(required=True)
