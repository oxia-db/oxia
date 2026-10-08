# Security Policy

The Oxia maintainers take security seriously and appreciate your efforts to responsibly disclose your
findings.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions or pull
requests.**

Instead, report them privately through GitHub's
[private vulnerability reporting](https://github.com/oxia-db/oxia/security/advisories/new) form. Use the
same form for vulnerabilities in any other repository of the [oxia-db](https://github.com/oxia-db)
organization (client libraries, Helm charts, etc.) and mention the affected repository in the report.

Please include in your report:

* A description of the vulnerability and of its potential impact
* The affected component and version(s)
* Steps to reproduce the issue, or a proof of concept
* Any known mitigation or suggested fix

## Response Process

The maintainers will acknowledge your report within **3 business days** and will provide an initial
assessment, with an estimated timeline for a fix, within **10 business days**.

We will keep you informed of the progress toward a fix and may ask you for additional information.

## Disclosure Policy

Oxia follows a coordinated disclosure process. When a vulnerability is confirmed, the maintainers will:

1. Develop and test a fix privately
2. Request a CVE identifier, if appropriate
3. Release a patched version
4. Publish a [security advisory](https://github.com/oxia-db/oxia/security/advisories), crediting the
   reporter unless they prefer to remain anonymous

We ask you to keep the details of the vulnerability private until the advisory is published.

## Supported Versions

Security fixes are released as a patch release of the latest minor version of Oxia. They may also be
backported to an earlier minor version that still has an active `release-X.Y` branch, at the discretion
of the maintainers.

We recommend running the latest [release](https://github.com/oxia-db/oxia/releases).

## Security Response Team

The Oxia [maintainers](MAINTAINERS.md) act as the security response team of the project and handle the
reports according to this policy. See [GOVERNANCE.md](GOVERNANCE.md) for more details.
