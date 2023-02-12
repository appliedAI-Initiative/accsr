# accsr: Simple tools for loading data and configurations 

This lightweight library contains utilities for managing, loading, uploading, opening and generally wrangling data and
configurations. It was battle tested in multiple projects at appliedAI. 

The main useful abstractions provided by this library are:
1. The `RemoteStorage`
class for a git-like, programmatic access to data stored in any cloud storage.
2. The configuration module for a simple, yet powerful configuration management.

## Overview

Source code documentation and usage examples are [here](https://appliedai-initiative.github.io/accsr/docs/).

## Installation

Install the latest release with
```shell script
pip install accsr
```

To live on the edge, install the latest develop version with
```shell script
pip install --pre accsr
```

## Contributing

Please open new issues for bugs, feature requests and extensions. See more details about the structure and
workflow in the [developer's readme](README_dev.md). The coverage and pylint report can be found on the project's
[github pages](https://appliedai-initiative.github.io/accsr/).

