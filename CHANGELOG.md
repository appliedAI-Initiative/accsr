# Changelog

## 0.1.3 Bugfix release

### Bugfixes:
- Fixed regression bug of RemoteStorage allowing for support of different providers
- Fixed link resolution in docu and faulty default parameter in sql_access

## 0.1.2

### Improvements:
- extension of RemoteStorage, necessary e.g. for accessing private storage hosts
- improvement of conversion module through documentation and unittests


## 0.1.1

### Features:
- config module for simplifying creation of programmatically accessible configurations


## 0.1.0 - Initial release

### Features:

- RemoteStorage abstraction for interacting with different types of storage providers
- loading module for downloading and opening files
- sql_access module providing some best practices for accessing databases
- conversions module, currently only containing helpers for converting to json

