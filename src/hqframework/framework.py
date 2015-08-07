import random
from threading import Thread, Event
import logging
import json
import uuid
import datetime

from abc import ABCMeta, abstractmethod
import cherrypy
import yaml
from schematics.models import Model
from schematics.types import StringType
from schematics.exceptions import ModelValidationError, ModelConversionError
from hqlib.sql import Base
from yaml import YAMLError

from hqframework.messaging import JobProcessSubscriber
from hqframework.exceptions import LaunchTaskException, GetWorkersException
from hqlib.sql.models import Worker
from hqlib.sql.models import Job
from hqlib.rabbitmq.rpc import RPCPublisher
from hqframework.messaging import JobPublisher
from pika.exceptions import AMQPError


class FrameworkUtils(object):
    def __init__(self, rabbitmq):
        self.rabbitmq = rabbitmq
        self.logger = logging.getLogger("hq.framework.utils")

    def jsonify_error(self, status, message, traceback, version):
        response = cherrypy.response
        response.headers['Content-Type'] = 'application/json'
        data = {'status': status, 'message': message}

        headers = cherrypy.request.headers

        if 'X-Debug' in headers:
            data['traceback'] = traceback

        return json.dumps(data)

    def auth(self, permission=None, debug=False):
        headers = cherrypy.request.headers

        if 'X-Auth-Token' not in headers:
            raise cherrypy.HTTPError(400, "Missing API Token")

        token = headers['X-Auth-Token']

        publisher = RPCPublisher(self.rabbitmq, "security", "validate")
        output = {'token': token}

        if permission is not None:
            output['permission'] = permission

        correlation_id = publisher.publish(output)

        if correlation_id is None:
            raise cherrypy.HTTPError(500, "Error publishing auth rpc")

        data = publisher.get_data(correlation_id)

        if data is None:
            raise cherrypy.HTTPError(500, "Did not hear back from a manager - security validate")

        if data['code'] != 200:
            raise cherrypy.HTTPError(data['code'], data['error'])

        cherrypy.request.user = {'id': data['user']['id'], 'name': data['user']['name']}

class AbstractFramework(Thread):
    __metaclass__ = ABCMeta

    def __init__(self, name, job_class):
        super(AbstractFramework, self).__init__()
        self.name = name
        self.job_class = job_class
        self.rabbitmq = None
        self.database = None
        self.config_path = None
        self.config = None
        self.id = None
        self.event = Event()
        self.logger = logging.getLogger("hq.framework")

    def register_framework(self, rabbitmq, database, config_path):
        self.config_path = config_path
        self.rabbitmq = rabbitmq
        self.database = database

        if not self.load_config(self.config_path):
            return False

        tries = 0

        while self.id is None:
            tries += 1
            if tries >= 5:
                self.logger.error("Unable to register framework. Stopping")
                self.stop()
                return False

            self.logger.info("Trying to register framework " + self.name)

            publisher = RPCPublisher(self.rabbitmq, "framework", "register")
            corr_id = publisher.publish({"name": self.name})

            if corr_id is None:
                self.logger.warning("RPC Framework Register corr_id is None")
                continue

            data = publisher.get_data(corr_id)

            if data is None:
                self.logger.warning("RPC Framework Register data is None")
                continue

            self.id = uuid.UUID(data['id'])
            self._registered()
            break

        JobProcessSubscriber(self.rabbitmq, self).start()

        return True

    def config_class(self):
        class ConfigClass(Model):
            datacenter = StringType(required=True)

        return ConfigClass

    def load_config(self, path):

        if path is not None:
            try:
                with open(self.config_path, "r") as f:
                    try:
                        config = self.config_class()(yaml.load(f), strict=False)
                    except ModelConversionError as e:
                        self.logger.error(
                            "Could not create config for framework " + self.name + " " + json.dumps(e.message))
                        return False
                    except YAMLError as e:
                        self.logger.error("Could not load config for framework " + self.name + " " + str(e))
                        return False

                try:
                    config.validate()
                except ModelValidationError as e:
                    self.logger.error(
                        "Could not validate config for framework " + self.name + " " + json.dumps(e.message))
                    return False
            except IOError as e:
                self.logger.error("Could not load config for framework " + self.name + ". " + e.message)
                return False
        else:
            self.logger.warning("Config Path is not defined for framework " + self.name)
            return False

        self.config = config

        return True

    def stop(self):
        self.event.set()

    def run(self):
        # TODO: can we reduce this time?
        while not self.event.wait(random.randint(5, 60)):
            try:
                self.publish_jobs()
            except:
                self.logger.exception("Publish Jobs Exception")
        self.on_stop()

    def publish_jobs(self):
        with self.database.session() as session:

            # TODO: find a better way to do this
            try:
                connection = self.rabbitmq.syncconnection()
                channel = connection.channel()
                declare_ok = channel.queue_declare(queue=self.name + "-jobs", passive=True)
                declare_ok = declare_ok.method

                if declare_ok.message_count > 0:
                    self.logger.info(self.name + " Job Process queue is not empty. Not publishing jobs")
                    return

                channel.close()
                connection.close()
            except AMQPError:
                self.logger.exception("Error checking " + self.name + " Job Process queue length")
                return

            try:
                publisher = JobPublisher(self.rabbitmq, self)
                for job_object in session.query(self.job_class).join(Job, self.job_class.job_id == Job.id). \
                        filter(Job.datacenter == self.config.datacenter).filter(Job.stopped_at == None):
                    publisher.publish_job(job_object.job_id)
                publisher.close()
            except AMQPError:
                self.logger.exception("Error publishing " + self.name + " jobs")
                return

    @abstractmethod
    def on_stop(self):
        pass

    def _registered(self):
        with self.database.session() as session:
            Base.metadata.create_all(bind=session.get_bind())

        self.logger.info("Successfully registered framework " + self.name)
        self.start()

        self.registered()

    def unix_time_millis(self, dt):
        epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=dt.tzinfo)
        delta = dt - epoch
        return int(delta.total_seconds() * 1000.0)

    def tasks_have_status(self, job, status):

        for target in job.targets:
            for task in target.tasks:
                if task.status != status:
                    return False

        return True

    def some_task_has_status(self, job, status):

        for target in job.targets:
            for task in target.tasks:
                if task.status == status:
                    return True

        return False

    def launch_task(self, worker, task):
        self.logger.debug("launching task " + task.name)

        publisher = RPCPublisher(self.rabbitmq, "task", "launch")
        correlation_id = publisher.publish({'task_id': str(task.id), 'worker_id': str(worker.id)})

        if correlation_id is None:
            raise LaunchTaskException("Error publishing launch task rpc")

        data = publisher.get_data(correlation_id, wait=10)

        if data is None:
            raise LaunchTaskException("Did not receive a reply from a manager - launch task")

        if data['code'] != 200:
            raise LaunchTaskException(data['error'])

        return data['status']

    def get_workers(self, datacenter):
        self.logger.debug("Getting workers")

        publisher = RPCPublisher(self.rabbitmq, "worker", "get")
        correlation_id = publisher.publish({'framework': self.name, 'datacenter': datacenter})

        if correlation_id is None:
            raise GetWorkersException("Error publishing get workers rpc")

        data = publisher.get_data(correlation_id)

        if data is None:
            raise GetWorkersException("Did not receive a reply from a manager - get workers")

        workers = []

        for worker in data['workers']:
            workers.append(Worker(id=worker['id'], target=worker['target'], framework=worker['framework'],
                                  tags=worker['tags']))

        return workers

    @abstractmethod
    def process_job(self, job_id):
        pass

    @abstractmethod
    def registered(self):
        pass


class AbstractFrameworkAPI(object):
    __metaclass__ = ABCMeta

    def __init__(self, framework, mount_point):
        self.framework = framework
        self.mount_point = mount_point
        self.logger = logging.getLogger("hq.framework.api")

    def register(self):
        conf = {
            '/': {
                'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                'tools.auth.on': True,
                'tools.response_headers.on': True,
                'tools.response_headers.headers': [('Content-Type', 'application/json')]
            }
        }
        cherrypy.tree.mount(self, self.mount_point, conf)
        self.logger.info("Registered " + self.framework.name + " API")
