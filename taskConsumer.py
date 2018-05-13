#!/usr/bin/env python

import pandas as pd
import json, time, boto3, os

class TaskConsumer(object):

    def __init__(self, sourceDict):


        # Info about our source in a dict. {source: file, id : <sourceUUID>}
        self.sourceDict = json.loads(sourceDict)

        # Dataset, pristine
        self.dataFrame_initial = self.loadfile()

        # retries on looking for new jobs
        self.retries = 0


    def loadfile(self):

        # TODO - chunks?
        s3 = boto3.client('s3')
        csv = s3.download_file('drr-import-bucket', self.sourceDict["source"], 'input.csv')

        return pd.read_csv(csv, dtype=str)


    # keep consuming as long as there's tasks to consume
    def consume(self):

        while True:

            if self.retries == self.cfg.checkForNewTaskRetries:
                break
            else:
                self.getTask()


    def getTask(self):

        # get a task

        sqs = boto3.client("sqs", region_name="eu-west-1")

        # TODO - securely get queue url
        task_queue_url = os.getenv("SQS_TASK_QUEUE_URL")

        try:
            msg = sqs.receive_message(QueueUrl=task_queue_url, MaxNumberOfMessages=1)
        except:
            # if the response contains no tasks, wait and retry
            time.sleep(self.cfg.checkForNewTaskWaitTime)
            self.retries += 1
            return

        taskMsg = sqs.receive_message(QueueUrl=task_queue_url, MaxNumberOfMessages=1)

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
        print(uniqueItemsPerDimension)
        for dimCount in uniqueItemsPerDimension:
            structureSize *= dimCount

        rows = len(df)
        sparsity = (100 / structureSize) * rows

        # {"url":s3/somefileorother/data, "sparsityAfter: 90%, rowsAfter:1000, task:{"dim1":[item3, item2], dim2:[item4]}}
        result = {
            "source": self.sourceDict["url"],
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

