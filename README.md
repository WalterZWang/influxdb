# Python Interface Wrapper for Influxdb 

This repository contains the Python Interface to interact with influxdb and a tutorial written in Jupyter Notebook.

# Overview
[Influxdb](https://www.influxdata.com/blog/getting-started-python-influxdb/) is a time-series database.
Influxdb has official [documentation](https://influxdb-python.readthedocs.io/en/latest/index.html). The tool we developped is a wrapper that can help you easily write to and read from influxdb database as pandas dataframe, without writting SQL queries. 

# Code Usage
### Clone repository
```
git clone https://github.com/WalterZWang/influxdb.git
cd influxdb
```

### Set up the environment 
Set up the virtual environment with your preferred environment/package manager.

The instruction here is based on **conda**. ([Install conda](https://docs.anaconda.com/anaconda/install/))
```
conda create --name influxdb-env python=3.7 -c conda-forge -f requirements.txt
condo activate influxdb-env
```

### Repository structure
``interfacedb.py``: interface to interact with influxdb

``tutorial.ipynb``: a tutorial example of how to use this interface

The following two files are not necessary:

``access.py``: the way to encode and decode the username and password, you can use other methods to store your username and password of the databse safely

``gridCarbon.py``: Interface to collect grid carbon emission intensity from WattTime

### Running
Unfortunately, you cannot run ``tutorial.ipynb``, because you need username and password to interact with the influxdb interface, which we cannot share in this public repository

### Feedback

Feel free to send any questions/feedback to: [Zhe Wang](mailto:zwang@lbl.gov)

