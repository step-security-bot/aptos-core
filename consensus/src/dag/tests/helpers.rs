// Copyright © Aptos Foundation

use aptos_consensus_types::common::{Round, Author, Payload};
use aptos_types::aggregate_signature::AggregateSignature;

use crate::dag::types::{NodeCertificate, CertifiedNode, Node};


pub(crate) fn new_certified_node(
    round: Round,
    author: Author,
    parents: Vec<NodeCertificate>,
) -> CertifiedNode {
    let node = Node::new(1, round, author, 0, Payload::empty(false), parents);
    CertifiedNode::new(
        node.clone(),
        NodeCertificate::new(node.metadata().clone(), AggregateSignature::empty()),
    )
}

pub(crate) fn new_node(round: Round, timestamp: u64, author: Author, parents: Vec<NodeCertificate>) -> Node {
    Node::new(0, round, author, timestamp, Payload::empty(false), parents)
}
