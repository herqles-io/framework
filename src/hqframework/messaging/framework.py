import json

from hqlib.rabbitmq.routing import Subscriber as RoutingSubscriber
from hqlib.rabbitmq.routing import Publisher as RoutingPublisher


class JobProcessSubscriber(RoutingSubscriber):
    def __init__(self, rabbitmq, framework):
        super(JobProcessSubscriber, self).__init__(rabbitmq, "framework", framework.name + "-jobs",
                                                   framework.name + "-jobs", qos=1)
        self.framework = framework

    def message_deliver(self, channel, basic_deliver, properties, body):
        data = json.loads(body)

        job_id = data['job_id']

        self.framework.process_job(job_id)

        channel.basic_ack(basic_deliver.delivery_tag)


class JobPublisher(RoutingPublisher):
    def __init__(self, rabbitmq, framework):
        super(JobPublisher, self).__init__(rabbitmq, "framework", framework.name + "-jobs")

    def publish_job(self, job_id):
        self.publish({"job_id": job_id})
