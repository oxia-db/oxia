# Contributing to Oxia

Thank you so much for contributing to Oxia. We appreciate your time and help.
Here are some guidelines to help you get started.

## Code of Conduct

Be kind and respectful to the members of the community. Take time to educate
others who are seeking help. Harassment of any kind will not be tolerated.

## Questions

If you have questions regarding Oxia, feel free to start a new discussion: https://github.com/oxia-db/oxia/discussions/new/choose

## Filing a bug or feature

1. Before filing an issue, please check the existing issues to see if a
   similar one was already opened. If there is one already opened, feel free
   to comment on it.
1. If you believe you've found a bug, please provide detailed steps of
   reproduction, the version of Oxia and anything else you believe will be
   useful to help troubleshoot it (e.g. OS environment, configuration,
   etc...). Also state the current behavior vs. the expected behavior.
1. If you'd like to see a feature or an enhancement please create a new [idea
   discussion](https://github.com/oxia-db/oxia/discussions/new?category=ideas) with 
   a clear title and description of what the feature is and why it would be
   beneficial to the project and its users.

## Submitting changes

1. DCO: All commits must be signed off to certify the
   [Developer Certificate of Origin](https://developercertificate.org/). Use
   `git commit -s` to add the `Signed-off-by` trailer to your commits. The
   DCO check on the Pull Request (PR) will fail if any commit is missing it.
1. Tests: If you are submitting code, please ensure you have adequate tests
   for the feature. Tests can be run via `make test`.
1. Since this is golang project, ensure the new code is properly formatted to
   ensure code consistency. Run `make lint`.

### Quick steps to contribute

1. Fork the project.
1. Download your fork to your PC (`git clone https://github.com/your_username/oxia && cd oxia`)
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Make changes and run tests (`make test`)
1. Add them to staging (`git add .`)
1. Commit your changes (`git commit -s -m 'Add some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create new pull request
