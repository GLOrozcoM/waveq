## Source code

See blurbs about each package used.

## Table of contents
* [main](#main)
* [database](#database)
* [deprecated](#deprecated)
* [experimental](#experimental)
* [ingest](#ingest)
* [process](#process)
* [s3_file_links](#s3_file_links)
* [single_year_ingestions](#single_year_ingestions)

## main

Top level files (e.g. `hindcast_main.py`) are meant to be run in a Pyspark job.
Each uses the other packages to perform ingesting, processing, and writing 
to a data base. 

## database

These files deal with reading user credentials and writing processed work 
to a database. These credentials should be specified in an upper level credentials
directory.

## deprecated

First versions and non-optimized code can be found here.

## experimental

Files for testing processing ideas and benchmarking lie here.

## ingest

A simple module for ingesting s3 data can be found here.

## process

A module used to convert h5 data sets to Spark data frames can be found here.

## s3_file_links

This package contains a module used to generate csv files into `buoy_links` and `hindcast_links`.
These contain s3 file links for the cluster to read in data.

## single_year_ingestions

Sample single year ingestions can be found here. Note that these are not single year 
instances of final version files (e.g. `hindcast_main.py`). The general processing approach
does not change, but implementation has been optimized/corrected in final version files.