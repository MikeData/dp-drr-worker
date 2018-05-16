
# Dimension Recombination Reporter - Worker

WORK IN PROGRESS - 10% project.

The DRR is a microservice based project to analyse all permutations from structural alterations (combinations of dimension items added/removed)
that could be applied to a given source dataset. Presenting the findings as a human readable report.


The DRR Worker Service:

* Consumes a source message from an SQS queue that contains a url for a dimensioned csv.
* Loads that CSV into memory and begins operating a task consumer.
* Consumes task messages - modifying a copy of original data as per task instructions.
* Analyses (size and sparsity) of that dataset permutation and writes to results queue.
* Gets a new task. If no tasks left, shuts task consumer and looks to consume a new source message.

The idea is to let us run through a large number of tasks without the overhead of loading up a entire source dataset for each one.

## Configuration

| Environment variable        |  Description
| --------------------------- |  -----------
| AWS_REGION                  | an AWS credential
| AWS_SECRET_ACCESS_KEY       | an AWS credential
| AWS_ACCESS_KEY_ID           | an AWS credential
| SQS_SOURCE_QUEUE_URL        | the full url of the drr source queue
| SQS_TASK_QUEUE_URL          | the full url of the drr task queue
| SQS_TASK_QUEUE_URL          | the full url of the drr result queue
| DRR_IMPORT_BUCKET           | the name of the import bucket