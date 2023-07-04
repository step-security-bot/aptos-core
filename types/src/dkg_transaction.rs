// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use move_core_types::value::MoveValue;
use serde::{Deserialize, Serialize};
#[cfg(any(test, feature = "fuzzing"))]
use proptest_derive::Arbitrary;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "fuzzing"), derive(Arbitrary))]
pub struct DKGTranscript {
    // dkg todo: fill in the fields
    pub dummy_bytes: Vec<u8>,
}

impl DKGTranscript {
    pub fn new() -> Self {
        Self {
            dummy_bytes: vec![0],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "fuzzing"), derive(Arbitrary))]
pub struct DKGTransaction {
    pub epoch: u64,
    // dkg todo: fill in the fields
    pub dkg_transcript: DKGTranscript,
}

impl DKGTransaction {
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            dkg_transcript: DKGTranscript::new(),
        }
    }

    pub fn get_move_args(self) -> Vec<MoveValue> {
        vec![
            MoveValue::U64(self.epoch),
            MoveValue::Vector(
                self.dkg_transcript
                    .dummy_bytes
                    .iter()
                    .map(|x| MoveValue::U8(*x))
                    .collect(),
            ),
        ]
    }
}
