# Adopters

This file lists the organizations and the projects that are using Oxia. It gives the community, and
anyone who is evaluating Oxia, a view of how the project is adopted. See the CNCF TOC FAQ for the
[definition of an adopter](https://github.com/cncf/toc/blob/main/FAQ.md#what-is-the-definition-of-an-adopter).

## Adopters List

| Adopter                   | Type             | Adoption Level | Description |
|---------------------------|------------------|----------------|-------------|
| [Apache Pulsar][pulsar]   | Project          | Production     | Oxia is the [recommended metadata store][pulsar-metadata] and coordination service for new Pulsar clusters, as an alternative to Apache ZooKeeper |
| [StreamNative Ursa][ursa] | Service Provider | Production     | Oxia is the metadata and coordination layer of Ursa, the Kafka-compatible, lakehouse-native data streaming engine that runs in production on StreamNative Cloud. The offset index, the fencing state and the consumer group state of every partition are committed to Oxia ([paper][ursa-paper]) |

[pulsar]: https://pulsar.apache.org
[pulsar-metadata]: https://pulsar.apache.org/docs/administration-metadata-store/
[ursa]: https://streamnative.io/ursa
[ursa-paper]: https://www.vldb.org/pvldb/vol18/p5184-guo.pdf

The types and the adoption levels follow the CNCF
[adopters template](https://github.com/cncf/project-template/blob/main/ADOPTERS.md):

* **CNCF End-User Member**: a [CNCF End-User member](https://www.cncf.io/people/end-user-community/)
  that uses Oxia internally
* **End-User Organization**: uses Oxia internally, or builds upon it, without selling it as a service
* **Service Provider**: offers Oxia, or a product that has Oxia as a core component, as a service
* **Project**: open source project that integrates with Oxia or depends on it
* **Consultancy**: assists others in developing solutions that use Oxia

The adoption level is one of: Production, Dev/Test, Trialing, Evaluating.

## Adding Yourself as an Adopter

If you are using Oxia, please let the community know by opening a pull request that adds a row to the
table above. If your organization needs to remain anonymous, you can list its industry instead of its
name (e.g. "Fortune 500 financial services company").
