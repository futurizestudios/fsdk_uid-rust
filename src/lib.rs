use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};

const FSDK_FSUID_TIMESTAMP_DELTA_BITS: u8 = 48; // Number of bits used to represent the milliseconds passed since the unix timestamp when a FSUID was generated
const FSDK_FSUID_NODE_IDENTIFIER_BITS: u8 = 8; // Number of bits used to represent the node identifier number, used to prevent collisions between FSUID's and identify which decentralized FSUID node generated the FSUID
const FSDK_FSUID_NODE_COUNTER_BITS: u8 = 8; //  Number of bits used to represent the node counter, used to prevent collisions between FSUID's between the same node and determine the order of FSUID generation within the same millisecond

const FSDK_FSUID_MAX_TIMESTAMP_DELTA: u64 = (1 << FSDK_FSUID_TIMESTAMP_DELTA_BITS) - 1; // Max timestamp delta that can be represented with FSDK_FSUID_TIMESTAMP_DELTA_BITS before overflow occurs
const FSDK_FSUID_MAX_NODE_IDENTIFIER: u8 = ((1 << (FSDK_FSUID_NODE_IDENTIFIER_BITS - 1))) +  ((1 << (FSDK_FSUID_NODE_IDENTIFIER_BITS - 1)) - 1); // Max node identifier that can be represented with FSDK_FSUID_NODE_IDENTIFIER_BITS before overflow occurs
const FSDK_FSUID_MAX_NODE_COUNTER: u8 = ((1 << (FSDK_FSUID_NODE_COUNTER_BITS - 1))) +  ((1 << (FSDK_FSUID_NODE_COUNTER_BITS - 1)) - 1); // Max node counter that can be represented with FSDK_FSUID_NODE_COUNTER_BITS before overflow occurs

pub fn fsdkuid_get_current_unix_timestamp_milliseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("[ERROR in fsdkuid_get_current_unix_timestamp_milliseconds()] Cannot retrieve current unix timestamp since time went backwards, please check the current time on your system!")
        .as_millis() as u64
}

pub struct FsdkUidGenerator {
    node_identifier: u8,
    counter: AtomicU8,
}

impl FsdkUidGenerator {
    pub fn new(node_identifier: u8) -> Self {
        if node_identifier > FSDK_FSUID_MAX_NODE_IDENTIFIER {
            panic!("[ERROR in FsdkUidGenerator.new()] FSUID Instance Identifier should be between 0 and {}, but a greater value was specified!", FSDK_FSUID_MAX_NODE_IDENTIFIER);
        }

        FsdkUidGenerator {
            node_identifier,
            counter: AtomicU8::new(0),
        }
    }

    pub fn generate_i64(&self) -> i64 {

        let counter = self.counter.fetch_add(1, Ordering::SeqCst);

        if counter == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }

        let timestamp_delta = (fsdkuid_get_current_unix_timestamp_milliseconds() & FSDK_FSUID_MAX_TIMESTAMP_DELTA) as i64;
        
        let fsuid: i64 = (timestamp_delta << (FSDK_FSUID_NODE_IDENTIFIER_BITS + FSDK_FSUID_NODE_COUNTER_BITS))
            | ((self.node_identifier as i64) << FSDK_FSUID_NODE_COUNTER_BITS)
            | (counter as i64);

        fsuid
    }

    pub fn generate_fsuid(&self) -> FsdkUid {
        let fsuid_i64 = self.generate_i64();
        FsdkUid::new(fsuid_i64)
    }


}

pub struct FsdkUid {
    fsuid: i64,
}


impl FsdkUid {
    pub fn new(fsuid: i64) -> Self {
        FsdkUid { fsuid }
    }

    pub fn i64(&self) -> i64 {
        self.fsuid
    }

    pub fn timestamp_delta(&self) -> i64 {
        (self.fsuid >> (FSDK_FSUID_NODE_IDENTIFIER_BITS + FSDK_FSUID_NODE_COUNTER_BITS)) & FSDK_FSUID_MAX_TIMESTAMP_DELTA as i64
    }

    pub fn node_identifier(&self) -> u8 {
        ((self.fsuid >> FSDK_FSUID_NODE_COUNTER_BITS) & FSDK_FSUID_MAX_NODE_IDENTIFIER as i64) as u8
    }

    pub fn node_counter(&self) -> u8 {
        (self.fsuid & FSDK_FSUID_MAX_NODE_COUNTER as i64) as u8
    }

    pub fn utc_datetime(&self) -> DateTime<Utc> {
        let timestamp_delta = self.timestamp_delta();
        DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_millis(timestamp_delta as u64))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    

    #[test]
    fn test_fsdkuid_current_unix_timestamp_milliseconds_greater_than_zero() {
        assert_ne!(fsdkuid_get_current_unix_timestamp_milliseconds(), 0, "[fsdkuid_get_current_unix_timestamp_milliseconds() Error] Current unix timestamp should be greater than zero, please check system RTC clock!");
    }
    
    #[test]
    fn test_fsdkuid_current_unix_timestamp_milliseconds_time_rewind() {
        let time_first = fsdkuid_get_current_unix_timestamp_milliseconds();
        std::thread::sleep(Duration::from_millis(1));
        let time_second = fsdkuid_get_current_unix_timestamp_milliseconds();

        assert!(time_first < time_second, "[fsdkuid_get_current_unix_timestamp_milliseconds() Error] Current unix timestamp should be greater than the previous one, please check system RTC clock!");
    }

    #[test]
    fn test_fsdkuid_fields() {
        let fsuid = FsdkUid::new(113131996488794368);
        assert_eq!(fsuid.i64(), 113131996488794368, "[fsuid.i64() Error] FSUID->i64 field must be 113131996488794368 but it contains another value");
        assert_eq!(fsuid.timestamp_delta(), 1726257270642, "[fsuid.timestamp_delta() Error] FSUID->timestamp_delta field must be 1726257270642 but it contains another value");
        assert_eq!(fsuid.node_identifier(), 1, "[fsuid.node_identifier() Error] FSUID->node_identifier field must be 1 but it contains another value");
        assert_eq!(fsuid.node_counter(), 0, "[fsuid.node_counter() Error] FSUID->node_counter field must be 0 but it contains another value");
    }

    

    #[test]
    fn test_fsdkuid_generator() {
        let fsuid_generator = FsdkUidGenerator::new(1);
        let fsuid = fsuid_generator.generate_fsuid();
        println!("fsuid: {}", fsuid.i64());
        println!("timestamp_delta: {}", fsuid.timestamp_delta());
        println!("node_identifier: {}", fsuid.node_identifier());
        println!("node_counter: {}", fsuid.node_counter());
        println!("utc_datetime: {}", fsuid.utc_datetime());
    }

    #[test]
    fn test_fsdkuid_generator_samenode_sequencecollision() {
        let fsuid_generator = FsdkUidGenerator::new(0);
        let fsuid1 = fsuid_generator.generate_i64();
        let fsuid2 = fsuid_generator.generate_i64();
        assert_ne!(fsuid1, fsuid2, "[FsdkUidGenerator.generate_i64() Error] Two sequential generated FSUID on same node collided")
    }

    #[test]
    fn test_fsdkuid_generator_samenode_sequenceoverflow() {
        let fsuid_generator = FsdkUidGenerator::new(0);

        let first_fsuid = fsuid_generator.generate_i64();
        let second_fsuid: i64 = fsuid_generator.generate_i64();
        let mut third_fsuid: i64 = fsuid_generator.generate_i64();

        for _ in 0..253 {
            
            third_fsuid = fsuid_generator.generate_i64();
        }

        
        let pre_last_fsuid = fsuid_generator.generate_i64();
        let last_fsuid = fsuid_generator.generate_i64();


        let fsuid_1_first = FsdkUid::new(first_fsuid);
        let fsuid_1_next = FsdkUid::new(second_fsuid);
        let fsuid_1_last = FsdkUid::new(third_fsuid);
        let fsuid_2_first = FsdkUid::new(pre_last_fsuid);
        let fsuid_2_last = FsdkUid::new(last_fsuid);

        if fsuid_1_first.timestamp_delta() == fsuid_1_next.timestamp_delta() {
            assert_eq!(fsuid_1_first.node_counter(), 0);
            assert_eq!(fsuid_1_next.node_counter(), 1);
            assert_eq!(fsuid_1_last.node_counter(), 255);
        }

        if fsuid_2_first.timestamp_delta() == fsuid_2_last.timestamp_delta() {
            assert_eq!(fsuid_2_first.node_counter(), 0);
            assert_eq!(fsuid_2_last.node_counter(), 1);
            
        }

        assert_ne!(fsuid_1_first.timestamp_delta(), fsuid_2_first.timestamp_delta());
        assert_eq!(fsuid_1_first.node_counter(), fsuid_2_first.node_counter());
        assert_eq!(fsuid_1_first.node_identifier(), fsuid_2_first.node_identifier());
    }

    #[test]
    fn test_fsdkuid_generator_differentnode_sequencecollision() {
        let fsuid_generator = FsdkUidGenerator::new(0);
        let fsuid_generator2 = FsdkUidGenerator::new(1);
        let fsuid1 = fsuid_generator.generate_i64();
        let fsuid2 = fsuid_generator2.generate_i64();
        assert_ne!(fsuid1, fsuid2, "[FsdkUidGenerator.generate_i64() Error] Two sequential generated FSUID on different nodes collided")
    }
}