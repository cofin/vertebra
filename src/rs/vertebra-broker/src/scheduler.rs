use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Debug)]
struct ClientState<T> {
    queue: VecDeque<T>,
    deficit: f64,
    weight: f64,
}

impl<T> ClientState<T> {
    fn new(weight: f64) -> Self {
        Self {
            queue: VecDeque::new(),
            deficit: 0.0,
            weight,
        }
    }
}

pub struct FairScheduler<T> {
    clients: Mutex<std::collections::HashMap<String, ClientState<T>>>,
    active_clients: Mutex<VecDeque<String>>,
    quantum: f64,
}

impl<T> FairScheduler<T> {
    pub fn new(quantum: f64) -> Self {
        Self {
            clients: Mutex::new(std::collections::HashMap::new()),
            active_clients: Mutex::new(VecDeque::new()),
            quantum,
        }
    }

    pub fn enqueue(&self, client_id: &str, item: T, weight: f64) {
        let mut clients = self.clients.lock().unwrap();
        let client = clients
            .entry(client_id.to_string())
            .or_insert_with(|| ClientState::new(weight));

        // Update weight if changed
        client.weight = weight;

        let was_empty = client.queue.is_empty();
        client.queue.push_back(item);

        if was_empty {
            let mut active = self.active_clients.lock().unwrap();
            if !active.contains(&client_id.to_string()) {
                active.push_back(client_id.to_string());
            }
        }
    }

    pub fn next(&self) -> Option<(String, T)> {
        let mut active = self.active_clients.lock().unwrap();
        let mut clients = self.clients.lock().unwrap();

        while let Some(client_id) = active.front().cloned() {
            let client = clients.get_mut(&client_id).unwrap();

            if client.deficit <= 0.0 {
                client.deficit += self.quantum * client.weight;
            }

            // Simplified DRR: assume cost=1.0 for all items for now
            let cost = 1.0;

            if client.deficit >= cost && !client.queue.is_empty() {
                let item = client.queue.pop_front().unwrap();
                client.deficit -= cost;
                if client.queue.is_empty() {
                    active.pop_front();
                }
                return Some((client_id, item));
            } else {
                // Not enough deficit, or empty (should be removed if empty)
                if client.queue.is_empty() {
                    active.pop_front();
                } else {
                    // Rotate to next client
                    let id = active.pop_front().unwrap();
                    active.push_back(id);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fairness() {
        let scheduler = FairScheduler::new(10.0);

        // Client A: weight 1.0
        scheduler.enqueue("A", 1, 1.0);
        scheduler.enqueue("A", 2, 1.0);

        // Client B: weight 2.0 (should get 2x service)
        scheduler.enqueue("B", 1, 2.0);
        scheduler.enqueue("B", 2, 2.0);
        scheduler.enqueue("B", 3, 2.0);
        scheduler.enqueue("B", 4, 2.0);

        // Sequence depends on quantum and weights
        // With quantum 10, A gets 10 credits, B gets 20.
        // Both queues small enough to drain completely in one pass if credits sufficient.

        let mut results = Vec::new();
        while let Some((client, item)) = scheduler.next() {
            results.push((client, item));
        }

        assert_eq!(results.len(), 6);
    }
}
