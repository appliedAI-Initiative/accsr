# Changelog

## 0.4.4
- Fixed bugs in RemoteStorage related to name collisions and serialization.
- Enhanced tests for RemoteStorage

## 0.4.3
- Reduced public interface of remote storage
- Fixed GH action for releasing to PyPi

## 0.4.2
Major first documented release.

### Features:
- Transactional safety and dry-run mode for RemoteStorage
- Extended support for glob and regex patterns in RemoteStorage
- Support for yaml files in Configuration
- Documentation in notebook for storage and configuration modules

### Improvements:
- Fixed and extended tests

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

