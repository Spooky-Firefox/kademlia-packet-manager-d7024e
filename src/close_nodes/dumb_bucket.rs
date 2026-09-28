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
            w_lock.swap_remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn contact(first_byte: u8, port: u16) -> Contact {
        let mut id = [0u8; 32];
        id[0] = first_byte;
        Contact {
            id,
            address: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    fn table() -> DumbBucket {
        DumbBucket {
            contacts: Arc::new(RwLock::new(Vec::new())),
        }
    }

    #[test]
    fn contacts_iter_yields_every_contact() {
        let table = table();
        assert_eq!(table.contacts_iter().count(), 0);
        table.maybe_add_contact(contact(0x80, 1000));
        table.maybe_add_contact(contact(0x40, 1000));
        let ids: Vec<u8> = table.contacts_iter().map(|c| c.id[0]).collect();
        assert_eq!(ids, vec![0x80, 0x40]);
    }

    #[test]
    fn remove_contact_removes_only_that_contact() {
        let table = table();
        table.maybe_add_contact(contact(0x80, 1000));
        table.maybe_add_contact(contact(0x40, 1000));

        table.remove_contact(&contact(0x80, 1001));
        assert_eq!(table.contacts_iter().count(), 2);

        table.remove_contact(&contact(0x80, 1000));
        let ids: Vec<u8> = table.contacts_iter().map(|c| c.id[0]).collect();
        assert_eq!(ids, vec![0x40]);
    }
}
