---
title: "Deploy Dagster as a service"
description: "Learn how to deploy Dagster as a service on a single machine"
---

# Deploy Dagster as a service

This guide will walk you through deploying Dagster as a service on a single machine. It includes instructions for setting up the Dagster webserver and daemon. This approach is suitable for small-scale deployments or for testing purposes. For production environments, consider using containerized deployments or cloud-based solutions

## Running the Dagster Webserver

The Dagster webserver is the core component of any Dagster deployment. It serves the Dagster UI and responds to GraphQL queries.

### Installation

First, install the Dagster webserver:

```bash
pip install dagster-webserver
```

### Starting the Dagster Webserver

To run the webserver, use the following command:

```bash
DAGSTER_HOME=/opt/dagster/dagster_home dagster-webserver -h 0.0.0.0 -p 3000
```

This configuration will:
- Set the `DAGSTER_HOME` environment variable, which tells Dagster where to store its persistent data and logs
- Write execution logs to `$DAGSTER_HOME/logs`
- Listen on `0.0.0.0:3000`

## Running the Dagster Daemon

The Dagster daemon is necessary if you're using schedules, sensors, backfills, or want to set limits on the number of runs that can be executed simultaneously.

### Installation

Install the Dagster daemon:

```bash
pip install dagster
```

### Starting the Daemon

Run the Dagster daemon with this command:

```bash
DAGSTER_HOME=/opt/dagster/dagster_home dagster-daemon run
```

The `dagster-daemon` process will periodically check your instance for:
- New runs to be launched from your run queue
- Runs triggered by your running schedules or sensors

:::note
Ensure that the `dagster-daemon` process has access to:
- Your `dagster.yaml` file
- Your `workspace.yaml` file
- The components defined on your instance
- The repositories defined in your workspace
:::

### Monitoring the Daemon

You can check the status of your `dagster-daemon` process in the Dagster UI:
1. Navigate to the Instance tab in the left-hand navigation bar
2. View the daemon status

:::important
A deployment can have multiple instances of `dagster-webserver`, but should include only a single `dagster-daemon` process.
:::

## Next Steps

Now that you have Dagster running as a service, you might want to explore:
- [Configuring your Dagster instance](/todo)
- [Setting up schedules and sensors](/guides/schedules)
