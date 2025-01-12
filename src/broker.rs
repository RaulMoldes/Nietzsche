//src/broker.rs module

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::Sender;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::thread;
type MessageId = u64; // type alias
type TopicId = u64; // type alias
pub type BrokerId = u64; // type alias
const MAX_MESSAGE_SIZE: usize = 1024; // Maximum message size. I have chosen 256 KB because this is the max allowed size of a topic message in AWS SQS. ON debug mode it will be of 1024 bytes

const MAX_MESSAGES: usize = 1000; // Maximum number of messages in a topic
const HEARTBEAT : u8 = 0x00; // Heartbeat byte
/// Message struct
#[derive(Debug, Clone)]
pub struct Message {
    pub content: String,
    pub length: usize,
    pub id: MessageId,
}
impl Message {
    pub fn new(content: &str, id: MessageId) -> Self {
        assert!(
            size_of::<Message>() <= MAX_MESSAGE_SIZE,
            "Message size exceeds the maximum allowed size of 256 KB"
        );

        Message {
            content: content.to_string(),
            length: content.len(),
            id,
        }
    }

    // Functions to serialize and deserialize the message
    pub fn to_bytes(&self) -> Vec<u8> {
        self.content.as_bytes().to_vec()
    }

    pub fn from_bytes(bytes: &[u8], id: MessageId) -> Self {
        let content = String::from_utf8(bytes.to_vec()).unwrap();
        let length = content.len();
        Message {
            content,
            length,
            id,
        }
    }
}

#[derive(Clone)]
pub struct Topic {
    pub id: TopicId,
    pub name: String,
    pub messages: VecDeque<Message>,
}

impl Topic {
    fn new(tid: TopicId, tname: &str) -> Self {
        Topic {
            id: tid,
            name: tname.to_string(),
            messages: VecDeque::new(),
        }
    }
    pub fn get_id(&self) -> &TopicId {
        &self.id
    }

    pub fn push(&mut self, message: Message) {
        // Push a message into the topic, ensuring that the topic is not full
        assert!(self.messages.len() < MAX_MESSAGES, "The topic is full!");
        self.messages.push_back(message); // FIFO QUEUE
    }

    pub fn poll(&self) -> Option<&Message> {
        // Poll the first message in the topic
        assert!(self.messages.len() > 0, "The topic is empty!");
        self.messages.front() // Get the first message
    }
}

/// Broker implementation
/// The broker is in charge of managing topics and messages
#[derive(Clone)]
pub struct Broker {
    pub broker_id: BrokerId,                                     // Unique broker id
    topics: Arc<Mutex<HashMap<TopicId, Arc<Mutex<Topic>>>>>, // Mutex to allow concurrent access to the topics
    // Each topic is wrapped in an Atomic Reference Counting (Arc) to allow multiple ownership (concurrency)
    next_topic: TopicId, // Points to the next topic id to be used
    sender: Sender<Vec<u8>>, // Sender to send heartbeats to the Zarathustra
}

impl Broker {
    pub fn new(bid: BrokerId, tx: Sender<Vec<u8>>) -> Self {
        Broker {
            broker_id: bid,
            topics: Arc::new(Mutex::new(HashMap::new())),
            next_topic: 0, // Initialize the next topic id to 0
            sender: tx
        }
    }

    fn insert_topic(&mut self, topic: Topic) {
        // A wrapper to insert a topic into the topics map
        // This ensures that the lock is released as soon as possible
        let mut topics = self.topics.lock().unwrap();
        topics.insert(topic.id, Arc::new(Mutex::new(topic)));
    }

    pub fn create_topic(&mut self, name: &str) -> TopicId {
        let topic = Topic::new(self.next_topic, name); // Create a new topic
                                                       // Get the current topic id
        self.insert_topic(topic); // Insert the topic into the topics map and free the lock
                                  // Get the current topic id
        let tid = self.next_topic;
        // Increment the next topic id
        self.next_topic += 1;
        println!("Topic '{}' created.", name);
        tid
    }
    fn get_topic(&self, id: TopicId) -> Option<Arc<Mutex<Topic>>> {
        // Lock the topics to get the topic by id
        let topics = self.topics.lock().unwrap();
        topics.get(&id).cloned()
    }

    // Function to get a mutable reference to a topic
    fn get_topic_mut(&self, id: TopicId) -> Option<Arc<Mutex<Topic>>> {
        // Lock the topics to get the topic by id
        let mut topics = self.topics.lock().unwrap();
        topics.get_mut(&id).cloned()
    }

    pub fn push_message(&self, topic_id: TopicId, message: Message) -> Result<(), &str> {
        // Lock the topics to push a message into a topic

        let some_topic = self.get_topic_mut(topic_id);
        if let Some(topic) = some_topic {
            let mut topic = topic.lock().unwrap();
            topic.push(message);
            Ok(())
        } else {
            Err("Topic not found!")
        }
    }

    pub fn consume(&self, topic_id: TopicId) -> Result<Message, &str> {
        // Lock the topics to consume a message from a topic
        let some_topic = self.get_topic_mut(topic_id);
        if let Some(topic) = some_topic {
            let topic = topic.lock().unwrap();
            let message = topic.poll().unwrap().clone();
            Ok(message)
        } else {
            Err("Topic not found!")
        }
    }

    pub fn send_heartbeat(&self) {
        // Send a heartbeat to the Zarathustra
        // Attempt to send one byte to the Zarathustra
        // THe byte should start with 0x00 to indicate that it is a heartbeat
        let tx = self.sender.clone();
        let _ = tx.send(vec![HEARTBEAT]);
     }

    pub fn start(self) {
        println!("Broker started.");
        // The broker must send periodic heartbeats to the Zarathustra (MAIN THREAD)
        // to indicate that it is alive

        thread::spawn(move || {
            loop {
                self.send_heartbeat();
                thread::sleep(std::time::Duration::from_secs(1)); // Add a sleep to avoid busy-waiting
            }
        });
    }
}
