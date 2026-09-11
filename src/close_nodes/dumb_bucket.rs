use crate::close_nodes::{CloseNodes, Contact, K, NodeId, xor_distance_cmp};
use std::sync::Arc;
use std::sync::RwLock;
pub struct DumbBucket {
    pub contacts: Arc<RwLock<Vec<Contact>>>,
}

impl CloseNodes for DumbBucket {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
        // TODO: implement a real dumb bucket
        // Sort the whole table before truncating: the k closest are only the
        // k closest if every contact was in the running.
        let mut v = self.contacts.read().unwrap().clone();
        v.sort_by(|a, b| xor_distance_cmp(a.id, b.id, id));
        v.truncate(K);
        v
    }

    fn maybe_add_contact(&self, contact: Contact) {
        let mut contacts = self.contacts.write().unwrap();
        contacts.push(contact);
    }
}
