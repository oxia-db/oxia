# Oxia Project Governance

Oxia is a scalable metadata store and coordination system for large-scale distributed systems. It is a
[Cloud Native Computing Foundation](https://www.cncf.io/) (CNCF) Sandbox project.

This document explains how the project is run. It applies to all the repositories in the
[oxia-db](https://github.com/oxia-db) GitHub organization.

- [Values](#values)
- [Maintainers](#maintainers)
- [Decision Making](#decision-making)
- [Code of Conduct](#code-of-conduct)
- [Security Response Team](#security-response-team)
- [Modifying this Document](#modifying-this-document)

## Values

The Oxia project and its maintainers embrace the following values:

* Openness: Communication and decision-making happen in the open and are discoverable for future
  reference. As much as possible, all discussions and work take place in public forums and open
  repositories.

* Fairness: All stakeholders have the opportunity to provide feedback and submit contributions, which
  will be considered on their merits.

* Community over Product or Company: Sustaining and growing our community takes priority over shipping
  code or sponsors' organizational goals. Each contributor participates in the project as an individual.

* Vendor Neutrality: The project direction and decisions are not controlled by any single organization.
  Maintainer selection, roadmap prioritization, and release decisions are made based on project merit,
  not employer affiliation.

* Inclusivity: We innovate through different perspectives and skill sets, which can only be
  accomplished in a welcoming and respectful environment.

* Participation: Responsibilities within the project are earned through participation, and every
  contributor can become a maintainer.

## Maintainers

Maintainers have write access to the repositories of the project. They review and merge pull requests,
triage issues, cut releases, and collectively manage the resources and the direction of the project.
The current maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

This privilege is granted with some expectation of responsibility: maintainers are people who care
about the Oxia project and want to help it grow and improve. A maintainer is not just someone who can
make changes, but someone who has demonstrated their ability to collaborate with the community, get the
most knowledgeable people to review code and docs, contribute high-quality code, and follow through to
fix issues.

The maintainers, collectively, are the governing body of the project.

### Becoming a Maintainer

Any contributor can become a maintainer. Candidates are expected to have demonstrated:

* Commitment to the project, with a sustained record of high-quality contributions over a period of
  several months: code, reviews, documentation, design discussions or helping other users
* A good understanding of the code base, the architecture and the processes of the project
* The ability to collaborate with the community, in line with the [Code of Conduct](#code-of-conduct)

A new maintainer is nominated by an existing maintainer on the private maintainers mailing list
(`cncf-oxia-maintainers@lists.cncf.io`), and the nomination is approved by a simple majority
[vote](#voting) of the current maintainers. Nominations are evaluated without prejudice to employer or
demographics, and should consider the organizational diversity of the maintainer group.

Once the nomination is approved, the new maintainer is added to [MAINTAINERS.md](MAINTAINERS.md), is
granted the necessary GitHub permissions, and is added to the CNCF maintainer records of the project.

### Removing a Maintainer

Maintainers may resign at any time if they feel that they will not be able to continue fulfilling their
project duties.

Maintainers may also be removed for inactivity, for failing to fulfill their responsibilities, or for
violating the Code of Conduct. Inactivity is defined as a period of very low or no activity in the
project for 12 months or more, with no definite schedule to return to full maintainer activity.

A maintainer may be removed at any time by a two-thirds majority [vote](#voting) of the other
maintainers.

### Emeritus Maintainers

Maintainers who resign, or who are removed for inactivity, are recognized for their past contributions
by being listed as emeritus maintainers in [MAINTAINERS.md](MAINTAINERS.md). Emeritus maintainers do not
have voting rights or write access to the repositories. They can be reinstated through the same process
that is used for new maintainers.

## Decision Making

Most of the decisions in Oxia are made by
"[lazy consensus](https://community.apache.org/committers/lazyConsensus.html)": a proposal, in the form
of a pull request, an issue or a discussion, is considered accepted when no maintainer objects to it
within a reasonable amount of time.

Changes to the code and to the documentation are made through pull requests, which are merged by the
maintainers following the process described in [CONTRIBUTING.md](CONTRIBUTING.md). Large or potentially
controversial changes should first be proposed in a GitHub
[issue](https://github.com/oxia-db/oxia/issues) or
[discussion](https://github.com/oxia-db/oxia/discussions), so that the community has the opportunity to
weigh in.

### Voting

When consensus cannot be reached, or where this document requires it, the maintainers decide by voting.
Any maintainer may call for a vote. Each maintainer has one vote.

Votes are taken in public on GitHub, in the relevant pull request, issue or discussion. Matters that need
to stay confidential (security reports, Code of Conduct incidents, and the nomination or the removal of
maintainers) are instead voted on the private maintainers mailing list.

Votes require a simple majority of all the current maintainers to succeed, except for the following
actions, which require a two-thirds majority of all the current maintainers:

* Removing a maintainer
* Modifying this governance document

## Code of Conduct

Oxia follows the [CNCF Code of Conduct](CODE_OF_CONDUCT.md). Violations can be reported privately to any
of the [maintainers](MAINTAINERS.md), or to the CNCF Code of Conduct Committee at <conduct@cncf.io>.

The maintainers handle the reports they receive in private. A maintainer who is directly involved in a
report does not take part in handling it, and the remaining maintainers will work with the CNCF Code of
Conduct Committee to resolve it.

## Security Response Team

The maintainers act as the security response team of the project. They are responsible for handling all
the reports of security vulnerabilities according to the [security policy](SECURITY.md).

## Modifying this Document

Changes to this document are proposed with a pull request and are approved by a two-thirds majority
[vote](#voting) of the maintainers.
