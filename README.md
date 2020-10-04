# WaveQ

A platform for easily visualizing and querying wave data to inform deployment of wave energy converters. 

## Table of contents
* [Overview](#Overview)
* [Tech stack](#Tech stack)

## Overview

Utility companies wanting to deploy wave energy converters need query-able wave data. Good 
wave data can inform where to place a wave energy converter. Wave data also helps monitor 
and predict wave conditions for wave energy converters. 

In August 2020, the Department of Energy (DOE) released a 2.7TiB wave hindcast and buoy data 
set containing wave characteristics along the Exclusive Economic Zone (EEZ) on the US west coast. 
Details regarding the data can be found on AWS(insert link) and the data's GitHub ReadME(insert link).
Data contain high resolution (up to hourly) read outs from over 600,000 points on the EEZ part of 
the US west coast and cover over 10 different wave variables. 

Performing analyses on the data is currently difficult. Data is divided into yearly files
in an H5 format. Variables are divided into data sets within each H5 file, and performing 
queries and visualizations without in depth knowledge of the H5 file format is almost impossible.

WaveQ offers an intuitive solution - an intuitive visualization platform with querying capabilities.

## Tech Stack

1. AWS S3 service stores raw data from the DOE.
2. PySpark reads in data and performs processing on data.
3. Data gets stored in TimescaleDB.
4. Grafana interacts with TimescaleDB to allow for easy visualization and querying.
