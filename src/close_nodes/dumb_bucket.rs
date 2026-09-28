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

    fn contacts_iter(&self) -> impl std::iter::Iterator<Item = Contact> {
        self.contacts.as_ref().read().unwrap().clone().into_iter()
    }

    fn remove_contact(&self, contact: &Contact) {
        let mut w_lock = self.contacts.write().unwrap();
        if let Some(i) = w_lock.iter().position(|c| c == contact) {
            w_lock.remove(i);
        }
    }
}
