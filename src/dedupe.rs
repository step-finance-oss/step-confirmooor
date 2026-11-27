use std::collections::HashSet;

use indexer_rabbitmq::geyser::Message;
use solana_transaction_status::EncodedTransaction;

/// Transaction signatures and the single statistics event have independent identities per slot.
pub(crate) struct SlotDedupe {
    signatures: HashSet<String>,
    seen_statistics: bool,
}

impl SlotDedupe {
    pub fn new(capacity: usize) -> Self {
        Self {
            signatures: HashSet::with_capacity(capacity),
            seen_statistics: false,
        }
    }

    pub fn is_duplicate(&mut self, message: &Message) -> bool {
        match message {
            Message::TransactionNotify(tx) => {
                let EncodedTransaction::Json(transaction) = &tx.transaction.transaction.transaction
                else {
                    return false;
                };
                transaction.signatures.first().is_some_and(|signature| {
                    !signature.is_empty() && !self.signatures.insert(signature.clone())
                })
            },
            Message::SlotStatisticsNotify(_) => std::mem::replace(&mut self.seen_statistics, true),
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        self.signatures.len() + usize::from(self.seen_statistics)
    }

    pub fn clear(&mut self) {
        self.signatures.clear();
        self.seen_statistics = false;
    }
}
