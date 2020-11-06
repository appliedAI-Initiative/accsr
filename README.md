# AppliedAI accsr Library

This lightweight library contains utilities for managing, loading, uploading, opening and generally wrangling data and
configurations. Its purpose is to be used across many different projects at appliedAI (and possibly somewhere else,
if we ever open source it). 

Please open new issues for bugs, feature requests and extensions. See more details about the structure and
workflow in the [developer's readme](README_dev.md).

## Overview

Source code documentation and usage examples are [here](http://resources.pages.aai.lab/accsr/docs/)

## Installation

The library is published to our [package registry](https://nexus.admin.aai.sh/#browse/browse:aai-pypi). Install
it from there with
```shell script
pip install accsr
```

To live on the edge, install the latest develop version with
```shell script
pip install --pre accsr
```

The standard installation comes without the sql access utilities.
If required, you can install them with
```shell script
pip install accsr[sql]
```
