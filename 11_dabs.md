# Declarative Automation Bundles

Formerly called Databricks Asset Bundles. DABs provide a structured framework via a machine-readable definition to complete your Databricks project. It integrates software engineering practices such as source control, testing, and CI/CD into the Databricks project. A Bundle puts metadata alongside the source code to describe what the project should look like in the form of a declarative YAML file and Databricks will make it happen. Hence, a Bundle is the end-to-end definition of a Databricks project.

**Databricks Project**: A collection of notebooks, scripts, test files, jobs & pipelines, and clusters

## Structure of a Bundle

A bundle is a folder tree containing source code and metadata describing the source code. 
Every bundle must contain exactly one configuration file named `databricks.yml` at the root of the project folder. It is the main file defining the bundle. Other YAML files are only read if referenced in databricks.yml via `include`

### databricks.yml

The databricks.yml has at least 3 top-level sections:
1) `bundle`: project name and high-level metadata
2) `resources`: declares source code such as jobs, pipelines, clusters
3) `targets`: environment specific settings (dev, test, prod)

Few more top-level sections:
4) `include`: to reference additional config files 
5) `variables`: define reusable values (like parameters)
6) `run_as`: specify identity to run jobs (user or service principal)
7) `artifacts`: external assets or libraries the bundle depends on (dependencies)
8) `sync`: include/exclude patterns to control which local files get uploaded to the workspace

A realistic `databricks.yml` file defining a bundle consisting of a single job into two different environments (dev and prod):

```yml
bundle:
  name: sales_etl

include:
  - resources/*.yml

variables:
  catalog:
    description: Unity Catalog catalog to write to
    default: dev_catalog

resources:
  jobs:                                     # defining the jobs to be included in this bundle - only 'daily_sales_job'
    daily_sales_job:                        # the bundle's internal key to refer to the job
      name: daily-sales-${bundle.target}    # the display name of the job in the workspace
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ../src/ingest.ipynb

targets:
  dev:
    mode: development
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    default: true
  prod:
    mode: production
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    variables:
      catalog: prod_catalog
    run_as:
      service_principal_name: etl-sp
```

The `databricks.yml` file is the entry point for the Bundle and what the Databricks CLI uses to configure the resources to be deployed from your local IDE to your workspace.

`mode`: A one-line declaration that the CLI uses to turn on some default behavious depending on the target environment. 

`mode: development` prefixes every non-file resource (such as a job) with `[dev ${current_user}]` and tags them with a `dev` tag. 
It also pauses all schedules and triggers on the jobs.

`development: production` validates that the bundle's Declarative Pipelines are marked `development: false`. It does not prefix resource names and does not pause schedules — the job deploys under its real name and runs on its real schedule.

### Important Databricks CLI Commands:

`databricks bundle init`: Initialize a new bundle in the current directory from a template (Python, SQL, MLflow).
Creates a starter `databricks.yml` file and project folder structure.

`databricks bundle generate`: To produce `databricks.yml` in an existing project.

`databricks bundle validate`: Validates the configuration of your bundle before deployment. 
Ensures your `databricks.yml` file is correctly structured. Catches missing or invalid fields early. Good practice to run before deploying

`databricks bundle deploy -t <target>`: Upload source code (such as notebooks) and create the resources (such as jobs) in the workspace. 
Creates a hidden `.bundle` folder in the workspace with your files.

`databricks bundle run <resource_key>`: Run a deployed job or pipeline and stream its output.

`databricks bundle destroy -t <target>`: Delete the deployed resources and uploaded files for that target
