// zarathustra.rs
// Zarathustra is the BrokerManager of Nietzsche.
// It is responsible for managing broker distribution and message routing, locking and unlocking.
// I built it as a replacement of Zookeeper in Apache Kafka Library.

use crate::broker::{Broker, BrokerId, Topic};
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time;
use std::time::Duration;

const NUM_BROKERS: u8 = 3;

// Keeps track of broker health and removes dead brokers
struct BrokerManager {
    alive_brokers: Vec<BrokerId>,
    last_heartbeat: HashMap<BrokerId, u64>,
}

impl BrokerManager {
    pub fn new() -> Self {
        BrokerManager {
            alive_brokers: Vec::new(),
            last_heartbeat: HashMap::new(),
        }
    }

    fn get_last_heartbeat(&self, id: BrokerId) -> Result<u64, &str> {
        if let Some(heartbeat) = self.last_heartbeat.get(&id) {
            Ok(*heartbeat)
        } else {
            Err("Heartbeat not found")
        }
    }

    fn insert_heartbeat(&mut self, id: BrokerId) {
        let heartbeat = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_heartbeat.insert(id, heartbeat);
    }



    fn add_broker(&mut self, id: BrokerId) {
        self.alive_brokers.push(id);
    }

    fn reset(&mut self, alive_brokers: Vec<BrokerId>) {
        self.alive_brokers.clear();
        for id in alive_brokers {
            self.alive_brokers.push(id);
        }
    }

    fn check_brokers_health(&mut self) -> Vec<BrokerId> {
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut brokers_to_remove = Vec::new();

        for id in self.alive_brokers.iter() {
            if now - self.get_last_heartbeat(*id).unwrap() > 5 {
                // Broker is dead
                brokers_to_remove.push(*id);
            }
        }
        brokers_to_remove
    }

  

}

pub struct Zarathustra {
    brokers: Arc<Mutex<HashMap<BrokerId, Arc<Mutex<Broker>>>>>, // HashMap of BrokerId and Broker. The whole hashmap does need to be locked because we are writing to it when creating a broker and when deleting a broker.
    next_broker_id: BrokerId,
    channels: Arc<Mutex<HashMap<BrokerId, Arc<Mutex<Receiver<Vec<u8>>>>>>>,
    broker_manager: Arc<Mutex<BrokerManager>>,
}



impl Zarathustra {
    pub fn new() -> Self {
        Zarathustra {
            brokers: Arc::new(Mutex::new(HashMap::new())),
            next_broker_id: 0,
            channels: Arc::new(Mutex::new(HashMap::new())),
            broker_manager: Arc::new(Mutex::new(BrokerManager::new())),
        }
    }



    // Broker management functions
    // Functions to acquire and release the lock on the brokers HashMap
    fn acquire_brokers_lock(
        &self,
    ) -> Result<MutexGuard<HashMap<BrokerId, Arc<Mutex<Broker>>>>, &str> {
        match self.brokers.lock() {
            Ok(guard) => Ok(guard),
            Err(_) => Err("Failed to acquire lock"),
        }
    }

    fn acquire_channels_lock(
        &self,
    ) -> Result<MutexGuard<HashMap<BrokerId, Arc<Mutex<Receiver<Vec<u8>>>>>>, &str> {
        match self.channels.lock() {
            Ok(guard) => Ok(guard),
            Err(_) => Err("Failed to acquire lock"),
        }
    }

    fn acquire_broker_manager_lock(&self) -> Result<MutexGuard<BrokerManager>, &str> {
        match self.broker_manager.lock() {
            Ok(guard) => Ok(guard),
            Err(_) => Err("Failed to acquire lock"),
        }
    }

    fn get_channel_shared(&self, id: BrokerId) -> Result<Arc<Mutex<Receiver<Vec<u8>>>>, &str> {
        let channels_lock = self.acquire_channels_lock()?;
        if let Some(channel) = channels_lock.get(&id) {
            Ok(Arc::clone(channel)) // Clone the Arc (increments reference count)
        } else {
            Err("Channel not found")
        }
    }

    // Function to get a shared reference to a broker
    fn get_broker_shared(&self, id: BrokerId) -> Result<Arc<Mutex<Broker>>, &str> {
        let brokers_lock = self.acquire_brokers_lock()?;
        if let Some(broker) = brokers_lock.get(&id) {
            Ok(Arc::clone(broker)) // Clone the Arc (increments reference count)
        } else {
            Err("Broker not found")
        }
    }

    // Function to get a mutable reference to a broker
    fn get_broker(&self, id: BrokerId) -> Result<Broker, &str> {
        let brokers_lock = self.acquire_brokers_lock()?;
        if let Some(broker) = brokers_lock.get(&id) {
            match broker.lock() {
                Ok(guard) => Ok(guard.clone()),
                Err(_) => Err("Failed to acquire lock"),
            }
        } else {
            Err("Broker not found")
        }
    }

    pub fn remove_broker(&mut self, id: BrokerId) {
        let mut brokers_lock = self.acquire_brokers_lock().unwrap();
        brokers_lock.remove(&id);
        // Release the mutex
    }

    pub fn create_broker(&mut self) -> BrokerId {
        let broker_id = self.next_broker_id;
        let (tx, rx) = mpsc::channel();
        self.insert_channel(broker_id, rx);
        self.insert_broker(broker_id, tx);
        self.next_broker_id += 1;
        broker_id
    }
    pub fn start_broker(&self, id: BrokerId) {
        let broker_shared = self.get_broker_shared(id).unwrap();
        thread::spawn(move || loop {
            let locked = broker_shared.lock().unwrap().clone();
            locked.start();
            thread::sleep(Duration::from_secs(1));
        });
    }

    // Heartbeat functions
    fn receive_heartbeat(&mut self, id: BrokerId) {
        let mut broker_manager = self.acquire_broker_manager_lock().unwrap();
        // heartbeat is received, insert current timestamp:
        broker_manager.insert_heartbeat(id);
    }

    // Wrapper function to reset the broker manager
    fn reset_broker_manager(&mut self) {
        let alive_brokers = self.acquire_brokers_lock().unwrap().keys().cloned().collect();
        let mut broker_manager = self.acquire_broker_manager_lock().unwrap();
        broker_manager.reset(alive_brokers);
    }


    // Remove dead brokers
    fn remove_dead_brokers(&mut self) {
        let mut broker_manager = self.acquire_broker_manager_lock().unwrap();
        let brokers_to_remove = broker_manager.check_brokers_health();
        drop(broker_manager); // Release the lock before mutably borrowing self
        for id in brokers_to_remove {
            self.remove_broker(id);
        }
        self.reset_broker_manager();
    }
   
    fn insert_broker(&mut self, id: BrokerId, channel: Sender<Vec<u8>>) {
        let mut brokers_lock = self.acquire_brokers_lock().unwrap();
        let broker_guard = Arc::new(Mutex::new(Broker::new(id, channel)));
        brokers_lock.insert(id, broker_guard);
        // Release the mutex
    }

    fn insert_channel(&mut self, id: BrokerId, channel: Receiver<Vec<u8>>) {
        let mut channels_lock = self.acquire_channels_lock().unwrap();
        channels_lock.insert(id, Arc::new(Mutex::new(channel)));
    }


}




// FN START();
fn start_zarathusthra(zrt: &mut Zarathustra) {
    // TODO: Implement the main loop of Zarathustra
    // The main loop should:
    // 1. Receive heartbeats from brokers
    // 2. Remove dead brokers
    // 3. Start new brokers
    // 4. Send heartbeats to brokers
    // 5. Handle broker messages
}