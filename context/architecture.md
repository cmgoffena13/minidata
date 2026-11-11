# Architecture

## Repo Purpose

Ingest data from multiple sources using Polars into Delta Tables.

## High Level Concepts
 - `Read`: read the data
 - `Validate`: validate the data against the schema if necessary
 - `Write`: write the data to a staging table
 - `Audit`: audit the table, check the grain and any other provided audits
 - `Publish`: append or merge the data into the target table
 - `Factory Pattern`: a scalable way to access different reader classes depending upon source config

## High Level Code Components
 - `src/sources`: holds source configuration
 - `src/processor.py`: holds `Processor` class that triggers multiple pipelines
 - `src/pipeline/runner.py`: holds `PipelineRunner` class that holds the pipeline logic

## Main Architecture Components
 - `Cloud Storage Bucket`: target data warehouse that holds delta tables (dev is local FS)

## Main Python Packages
 - `Polars`: primary ingestion tool
 - `Delta Table`: primary target table format
 - `Poldantic`: turn a pydantic schema into polars schema

## General Workflow
 1. Trigger `Processor.process()`
 2. Collect the source configs and trigger `PipelineRunner.run()` for each
 3. Aggregate the results and provide failure details if available