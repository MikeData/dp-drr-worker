
# Global imports
import boto3, datetime, time, json

# Local Imports
import config
from taskConsumer import TaskConsumer


# A wrapped dataframe, started by a consumer and acting as a consumer
class sourceConsumer(object):

    def __init__(self):
        self.created = datetime.datetime.now()
        self.cfg = config.newConfig()

        # TODO - not exacty subtle
        while True:
            self.getSource()
            time.sleep(10)  # seconds



    def getSource(self):

        sqs = boto3.client("sqs", region_name="eu-west-1")

        # TODO - securely get queue url
        queue_url =

        msg = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)

        sourceDict = json.loads(msg["Messages"][0]["Body"])

        consumer = TaskConsumer(sourceDict)
        consumer.consume()

        del consumer  # better to be safe

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg["Messages"][0]["ReceiptHandle"]
        )


# Start
sourceConsumer()



