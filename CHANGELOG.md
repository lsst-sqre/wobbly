# Change log

wobbly is versioned with [semver](https://semver.org/).
Dependencies are updated to the latest available version during each release, and aren't noted here.

Find changes for the upcoming release in the project's [changelog.d directory](https://github.com/lsst-sqre/wobbly/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-1.1.0'></a>
## 1.1.0 (2025-10-06)

### New features

- Add optional support for reporting exceptions to Sentry.

### Other changes

- Include the UWS job ID in metrics events so that timings can be traced back to the original request.
- Log usernames as `user` rather than `owner` in structured logging to match other services and make cross-matching easier.

<a id='changelog-1.0.0'></a>
## 1.0.0 (2025-06-17)

wobbly has been running in production for several months now, so release it as version 1.0.0. There are no user-visible changes in this release.

### Other changes

- Use [uv](https://github.com/astral-sh/uv) to maintain frozen dependencies and set up a development environment.

<a id='changelog-0.2.1'></a>
## 0.2.1 (2024-12-18)

### Bug fixes

- Disable strict validation of X.509 certificates when reporting metrics events. The certificates created by Strimzi do not currently pass those checks.

<a id='changelog-0.2.0'></a>
## 0.2.0 (2024-12-18)

### New features

- Add a `wobbly expire` command that deletes all jobs from the database that have passed their destruction time. This is run periodically as a Kubernetes `CronJob`.

<a id='changelog-0.1.0'></a>
## 0.1.0 (2024-12-12)

Initial release integrated with Safir 9.0.1.
