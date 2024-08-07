# Changelog

## 0.4.8
- Extended `RemoteStorage` to support adding `extra` to uploaded objects, as well as
customizing the extraction of the remote hash. This is especially useful for azure blobs
storage, since there the attribute `.hash` of the remote object doesn't coincide
with the md5 hash of the local file. Using the extra and retrieving the hash from the
metadata allows to circumvent this issue.


## 0.4.7
 - In `RemoteStorage` now absolute paths can be passed to pull to reference a remote_path. The most convenient way of using this new option is to always pass an absolute path as local_base_path
- 

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

