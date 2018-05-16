
# Global imports
import boto3, time, json, os
from time import gmtime, strftime

# Local Imports
from taskConsumer import TaskConsumer


# A wrapped dataframe, started by a consumer and acting as a consumer
class sourceConsumer(object):

    def __init__(self):
        self.created = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        # TODO - not exacty subtle
        while True:
            self.getSource()
            time.sleep(10)  # seconds



    def getSource(self):

        sqs = boto3.client("sqs", region_name="eu-west-1")

        source_queue_url = os.getenv("SQS_SOURCE_QUEUE_URL")

        msg = sqs.receive_message(QueueUrl=source_queue_url, MaxNumberOfMessages=1)

        if "Messages" in msg.keys():
            sourceDict = msg["Messages"][0]["Body"]

            consumer = TaskConsumer(sourceDict)
            consumer.consume()

            del consumer  # better to be safe

            sqs.delete_message(
                QueueUrl=source_queue_url,
                ReceiptHandle=msg["Messages"][0]["ReceiptHandle"]
            )

        else:
            print("No source message found at: ", strftime("%Y-%m-%d %H:%M:%S", gmtime()))


# Start
sourceConsumer()



