## abf - Airflow Backfill Tool

[![abf - Airflow Backfill Tool](https://github.com/sschaetz/abf/actions/workflows/pipeline.yml/badge.svg)](https://github.com/sschaetz/abf/actions/workflows/pipeline.yml)

### About

This command line tool can issue backfill commands to Airflow in small portions. The [Airflow](https://airflow.apache.org/) (1.0) scheduler can be knocked out easily if backfill commands are executed that request many DAG instanced to be backfilled.

The tool supports [Google Cloud Composer](https://cloud.google.com/composer) as well as a "normal"  Airflow installation.

### Usage & Commands

- `cb` Clean and Backfill Airflow - runs a `clean` command and then a `backfill` command
- `cb-gcp` Clean and Backfill Cloud Composer - runs a `clean` command and then a `backfill` command; uses `gcloud` CLI tool, requires [Cloud SDK](https://cloud.google.com/sdk) installed and authenticated


### GUI Operation

If the GUI extension [quick](https://github.com/szsdk/quick) is available on your system, you will be able to use the `--gui` flag to launch a GUI:

![Airflow Backfill Tool GUI](https://github.com/sschaetz/abf/blob/main/doc/af_bf.png?raw=true)
