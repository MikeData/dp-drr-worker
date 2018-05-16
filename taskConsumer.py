#!/usr/bin/env python

import pandas as pd
import json, time, boto3, os
from time import gmtime, strftime

class TaskConsumer(object):

    def __init__(self, sourceDict):


        # Info about our source in a dict. {source: file, id : <sourceUUID>}
        self.sourceDict = json.loads(sourceDict)

        # Dataset, pristine
        self.dataFrame_initial = self.loadfile()

        # retries on looking for new jobs
        self.checkForNewTaskRetries = 3
        self.checkForNewTaskWaitTime = 2
        self.retries = 0


    def loadfile(self):

        bucket = os.getenv("DRR_IMPORT_BUCKET")

       # TODO - chunks?
        s3 = boto3.client('s3')
        s3.download_file(bucket, self.sourceDict["Source"], 'input.csv')

        return pd.read_csv('input.csv', dtype=str)


    # keep consuming as long as there's tasks to consume
    def consume(self):

        while True:

            if self.retries == self.checkForNewTaskRetries:
                break
            else:
                self.getTask()


    def getTask(self):

        # get a task

        sqs = boto3.client("sqs", region_name="eu-west-1")

        task_queue_url = os.getenv("SQS_TASK_QUEUE_URL")

        taskMsg = sqs.receive_message(QueueUrl=task_queue_url, MaxNumberOfMessages=1)

        if "Messages" in taskMsg.keys():
            taskMsg = taskMsg["Messages"][0]["Body"]

        else:
            print("No task message found at: ", strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            time.sleep(self.checkForNewTaskWaitTime)
            self.retries += 1
            return

        task = json.loads(taskMsg)

        self.analyse(task)


    # Analyse the dataframe - generate one result
    def analyse(self, task):

        # crete a copy of the pristine dataset
        df = self.dataFrame_initial.copy()

        # for each specified dimension
        for dimension in task.keys():

            # drop each specified item from the dataframe
            for item in task[dimension]:
                print(dimension, item, df.columns.values)
                df = df[df[dimension] != item]


        # Calculate Sparsity
        # i.e multiply the unique items now in each dimension together
        uniqueItemsPerDimension = []
        for col in df.columns.values:
            uniqueItemsPerDimension.append(len(df[col].unique()))


        structureSize = 1
        for dimCount in uniqueItemsPerDimension:
            structureSize *= dimCount

        rows = len(df)
        sparsity = (100 / structureSize) * rows

        # {"url":s3/somefileorother/data, "sparsityAfter: 90%, rowsAfter:1000, task:{"dim1":[item3, item2], dim2:[item4]}}
        result = {
            "source": self.sourceDict["Source"],
            "sourceId": self.sourceDict["SourceId"],
            "sparsityAfter":sparsity,
            "rowsAfter":rows,
            "task":task
        }

        sqs = boto3.client("sqs", region_name="eu-west-1")

        # TODO - securely get queue url
        queue_url = os.getenv("SQS_RESULT_QUEUE_URL")

        jdump = json.dumps(result)

        sqs.send_message(QueueUrl=queue_url, MessageBody=jdump)

        # shouldn't be necessary, but better safe
        del df

