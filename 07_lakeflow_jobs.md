# Lakeflow Jobs

Formerly called Databricks Workflow/Jobs.

Spark Declarative Pipelines is an ETL Framework. Its unit of work is a dataset. The engine expects transformation definitions but manages everything around it such as checkpoints, schema locations, order of execution, incremental updates, etc.

Lakeflow Job is a managed orchestrator service provided by Databricks. Its unit of work is a Task. A Task is anything that's runnable: a notebook, a Python script, a SQL query, a dashboard refresh - or a pipeline. It requires explicitly declaring dependencies between tasks to decide the order of execution of the job.

A pipeline is usually a task within a job. The pipeline automates and manages the ETL framework. Pipelines can also schedule themselvs but a Job enables integrating that pipeline with other pipelines, non-pipelines steps before or after, or job-level notifications, etc. Jobs orchestrates heterogeneous tasks on a schedule; Spark Declarative Pipelines manages a tables within one of those tasks.

Whereas, Lakeflow Connect provides managed connectors for ingesting from Salesforce, Workday, SQL Server, and similar — the sources Auto Loader can't reach because they aren't files in object storage. 

**Canonical Pattern**: Connect ingests → Pipelines transforms → Jobs orchestrates the whole thing on a schedule.

An instance of execution of a Job is known as a Run.

## Components of a Job

1) Trigger

- Manual
- Schedule: CRON Job that triggers on fixed intervals
- File/Table Events: Trigger when a new file arrives or a table updates
- Continuous: Spark Streaming Job 

2) Tasks: Orchestration of tasks within a Job by declaring dependencies between them

3) Configuration

- Parameters: Optional parameters passed between Tasks
- Notifications: Slack, email, teams message on Job Events such as completes or failures
- Git Integration: Allows cloning git repositories as tasks
- Access Control: Allows role-based access to who gets to view, edit, or run the Job

4) Monitoring: Jobs UI enables both real-time and historical tracking of job runs

5) Retry/Rerun: Manual and automatic retry capabilities in the case of failures

## Tasks 

The following are the type of tasks we can define in a Job:

1) Code: Notebook, Python Script
2) SQL: Query, '.sql' File
3) Data Transformation: SDPs, Data Build Tool (open-source ETL framework)
4) Control Flow: Enable dynamic tasks via If-Else logic

### Task Properties

- Source: The source code such as location of the notebook or the .py/.sql file
- Compute: Serverless / Job / All purpose (Job is recommended)
- Dependencies: The other tasks that the selected task is dependent on
- Parameters: Optional dynamic values that can be passed into the task
- Metrifc Thresholds: Flagging unusual tasks based on exceeding certain thresholds such as Time Duration
- Notifications: Task-level notifications via Slack, Email, Teams, etc.