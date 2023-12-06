# Release Notes: 0.4.7-dev0

## Bugfix release
- Fixed bugs in RemoteStorage related to name collisions and serialization.
Previously it could happen that files would be re-pushed/pulled despite
already existing on the target.
- Enhanced tests for RemoteStorage
