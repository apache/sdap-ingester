# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
### Changed
### Deprecated
### Removed
### Fixed
- SDAP-423: Fixed verbosity settings not propagating to ingester subprocesses
- SDAP-417: Fixed bug where very spatially narrow tiles would have their WKT for the geo field represent the incorrect shape (ie a very narrow polygon being rounded to a line), which would cause an error on write to Solr.
### Security

## [1.0.0] - 2022-12-05
### Added
### Changed
 - SDAP-408: Improve L2 satellite data ingestion speed
   - Improved fault tolerance of writes to data & metadata stores. For ingestion pipelines that generate many tiles, the data stores may fail on some writes which was treated as an unrecoverable failure. Now more tolerant of this.
   - Batched writes: Reduced the number of network IO operations by consolidating writes of tile data + metadata.
   - Removed unnecessary function call. Removed an unneeded function call that seemed to be consuming a lot of pipeline runtime.
   - Batched tasks submitted to executors in pool. Saves wasted time switching between completed & new tasks.
- Added missing ASF headers to all .py files in this repository.
- Added ASF `README` for release.
### Deprecated
### Removed
### Fixed
### Security


