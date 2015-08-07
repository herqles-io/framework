import cherrypy
import os

from hqlib.sql import SQLDB
from hqlib.rabbitmq import RabbitMQ
from hqframework.framework import FrameworkUtils
from yaml import YAMLError
from hqframework.config import parse_config, BaseConfig, RabbitMQConfig, PathConfig, SQLConfig
from schematics.exceptions import ModelValidationError, ModelConversionError
import json
from hqlib.daemon import Daemon


class FrameworkDaemon(Daemon):

    def __init__(self, args):
        super(FrameworkDaemon, self).__init__("Framework")
        self.args = args
        self.path_config = None
        self.sql_config = None
        self.rabbitmq_config = None
        self.rabbitmq = None
        self.frameworks = []

    def setup(self):
        try:
            base_config = parse_config(self.args.config)
        except YAMLError as e:
            self.logger.error("Could not load worker config "+str(e))
            return False
        except IOError as e:
            self.logger.error("Could not load worker config "+e.message)
            return False

        try:
            base_config = BaseConfig(base_config, strict=False)
        except ModelConversionError as e:
            self.logger.error("Could not create base config " + json.dumps(e.message))
            return False

        try:
            base_config.validate()
        except ModelValidationError as e:
            self.logger.error("Could not validate base config " + json.dumps(e.message))
            return False

        try:
            self.path_config = PathConfig(base_config.paths, strict=False)
        except ModelConversionError as e:
            self.logger.error("Could not create path config " + json.dumps(e.message))
            return False

        try:
            self.path_config.validate()
        except ModelValidationError as e:
            self.logger.error("Could not validate path config " + json.dumps(e.message))
            return False

        try:
            self.sql_config = SQLConfig(base_config.sql, strict=False)
        except ModelConversionError as e:
            self.logger.error("Could not create sql config " + json.dumps(e.message))
            return False

        try:
            self.sql_config.validate()
        except ModelValidationError as e:
            self.logger.error("Could not validate sql config " + json.dumps(e.message))
            return False

        try:
            self.rabbitmq_config = RabbitMQConfig(base_config.rabbitmq, strict=False)
        except ModelConversionError as e:
            self.logger.error("Could not create rabbitmq config " + json.dumps(e.message))
            return False

        try:
            self.rabbitmq_config.validate()
        except ModelValidationError as e:
            self.logger.error("Could not validate rabbitmq config " + json.dumps(e.message))
            return False

        return True

    def run(self):
        database = SQLDB(self.sql_config.driver, self.sql_config.host, self.sql_config.port, self.sql_config.database,
                         self.sql_config.username, self.sql_config.password, self.sql_config.pool_size)
        database.connect()

        hosts = []
        for host in self.rabbitmq_config.hosts:
            (ip, port) = host.split(":")
            hosts.append((ip, int(port)))

        self.rabbitmq = RabbitMQ(hosts, self.rabbitmq_config.username, self.rabbitmq_config.password,
                                 self.rabbitmq_config.virtual_host)
        self.rabbitmq.setup_database()

        framework_utils = FrameworkUtils(self.rabbitmq)
        cherrypy.tools.auth = cherrypy.Tool("on_start_resource", framework_utils.auth)

        self.logger.info("Starting Framework")

        for config_name in os.listdir(self.path_config.framework_configs):

            if not config_name.endswith(".yml") and not config_name.endswith(".yaml"):
                continue

            config_path = self.path_config.framework_configs + "/" + config_name
            try:
                framework_config = parse_config(config_path)
            except YAMLError as e:
                self.logger.error("Could load framework config " + config_name + " " + str(e))
                continue

            if 'module' not in framework_config:
                self.logger.error("Framework config " + config_name + " does not have a module to load.")
                continue

            modules = framework_config['module'].split(".")

            try:
                module = __import__(framework_config['module'])
                modules.pop(0)
                for m in modules:
                    module = getattr(module, m)
                api = getattr(module, 'FrameworkAPI')
                framework = getattr(module, 'Framework')
                framework = framework()
                self.frameworks.append(framework)
                api = api(framework)
            except:
                self.logger.exception("Error loading framework module " + framework_config['module'])
                continue

            if not framework.register_framework(self.rabbitmq, database, config_path):
                self.logger.error("Framework " + framework.name + " could not register")
                continue

            api.register()

        if len(self.frameworks) == 0:
            self.logger.warning("No frameworks loaded")
            return False

        cherrypy.config.update({'engine.autoreload.on': False,
                                'engine.timeout_monitor.on': False,
                                'error_page.default': framework_utils.jsonify_error,
                                'server.socket_port': 8081})
        cherrypy.engine.start()

        return True

    def on_shutdown(self, signum=None, frame=None):
        cherrypy.engine.exit()
        for subscriber in list(self.rabbitmq.active_subscribers):
            subscriber.stop()
        for framework in list(self.frameworks):
            framework.stop()

    def on_reload(self, signum=None, frame=None):
        pass

    def get_pid_file(self):
        return self.path_config.pid

    def get_log_path(self):
        return self.path_config.logs


def main(args):
    daemon = FrameworkDaemon(args)
    daemon.start()
