//! Components written in Rust, for data DuckDB cannot reach.
//!
//! Each one implements a trait from `etl-plugin-sdk` and appears in [`all`].
//! The engine adds every connector listed here to its component registry, so a
//! connector needs no line anywhere else: the canvas palette, `etl components`,
//! validation, lineage, `etl run`, the scheduler, the console and a built
//! artifact all reach it through the engine.
//!
//! This crate depends on the SDK and on `etl-metadata`, and never on the
//! engine. What a connector is allowed to know about is records and its own
//! properties; SQL, plans and DuckDB are the engine's business.
//!
//! Delivery semantics for each connector are written down in
//! `docs/connectors.md`, which is the half of "done" a test cannot show.

use etl_plugin_sdk::Connector;
use std::sync::OnceLock;

mod aws;
#[cfg(test)]
mod fixture;
mod gcp;
pub mod graphql;
mod http;
pub mod kafka;
pub mod kinesis;
mod lease;
pub mod nats;
pub mod pubsub;
pub mod rest;
pub mod sqs;
mod tls;
pub mod xml;

/// Every native connector, in registry order.
pub fn all() -> &'static [(String, Connector)] {
    static ALL: OnceLock<Vec<(String, Connector)>> = OnceLock::new();

    ALL.get_or_init(|| {
        [
            Connector::Source(&xml::XmlSource),
            Connector::Sink(&xml::XmlSink),
            Connector::Source(&rest::RestSource),
            Connector::Sink(&rest::RestSink),
            Connector::Source(&graphql::GraphqlSource),
            Connector::Sink(&graphql::GraphqlSink),
            Connector::Source(&kafka::KafkaSource),
            Connector::Sink(&kafka::KafkaSink),
            Connector::Source(&nats::NatsSource),
            Connector::Sink(&nats::NatsSink),
            Connector::Source(&kinesis::KinesisSource),
            Connector::Sink(&kinesis::KinesisSink),
            Connector::Source(&sqs::SqsSource),
            Connector::Sink(&sqs::SqsSink),
            Connector::Source(&pubsub::PubsubSource),
            Connector::Sink(&pubsub::PubsubSink),
        ]
        .into_iter()
        .map(|connector| (connector.spec().id, connector))
        .collect()
    })
}

/// The connector for a component id, if it is a native one.
pub fn find(component_id: &str) -> Option<Connector> {
    all()
        .iter()
        .find(|(id, _)| id == component_id)
        .map(|(_, connector)| *connector)
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_metadata::Namespace;

    #[test]
    fn every_connector_is_in_the_namespace_its_direction_says() {
        // A source in `snk.*` would be offered as an output on the canvas and
        // then asked to read. The spec's namespace and the trait have to agree.
        for (id, connector) in all() {
            let namespace = connector.spec().namespace;
            match connector {
                Connector::Source(_) => assert_eq!(namespace, Namespace::Source, "{id}"),
                Connector::Sink(_) => assert_eq!(namespace, Namespace::Sink, "{id}"),
            }
        }
    }

    #[test]
    fn a_native_source_offers_columns() {
        for (id, connector) in all() {
            if let Connector::Source(_) = connector {
                assert!(
                    connector.spec().property("columns").is_some(),
                    "{id} has no `columns`, so nothing could ever give its fields a type"
                );
            }
        }
    }

    #[test]
    fn ids_are_unique_and_found_by_id() {
        let mut ids: Vec<&str> = all().iter().map(|(id, _)| id.as_str()).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), count);

        assert!(find("src.file.xml").is_some());
        assert!(find("snk.file.xml").is_some());
        assert!(
            find("src.file.csv").is_none(),
            "a DuckDB component is not native"
        );
    }
}
