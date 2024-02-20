use std::{time::{Duration, Instant}, collections::VecDeque};

/// 直近duration間のdataを保持する
#[derive(Debug)]
pub struct TimeQueue<T> {
    pub duration: Duration,
    pub data: VecDeque<(Instant, T)>,
}

impl<T> TimeQueue<T> {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            data: VecDeque::new(),
        }
    }

    pub fn push(&mut self, item: T) {
        let now = Instant::now();
        self.data.push_back((now, item));
    }

    pub fn extend<I>(&mut self, iter: I) where I: IntoIterator<Item = T> {
        let now = Instant::now();
        self.data.extend(iter.into_iter().map(|item| (now, item)));
    }

    pub fn first(&self) -> Option<&T> {
        self.data.front().map(|(_, item)| item)
    }

    pub fn last(&self) -> Option<&T> {
        self.data.back().map(|(_, item)| item)
    }

    pub fn get_data(&self) -> Vec<&T> {
        self.data.iter().map(|(_, item)| item).collect()
    }

    pub fn get_data_iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter().map(|(_, item)| item)
    }

    pub fn retain(&mut self) {
        let curr = Instant::now();
        while let Some((instant, item)) = self.data.pop_front() {
            if curr.duration_since(instant) <= self.duration {
                self.data.push_front((instant, item));
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[test]
fn test_time_queue() {
    let mut queue = TimeQueue::new(Duration::from_secs(5));
    queue.push(1);
    std::thread::sleep(Duration::from_secs(1));
    queue.push(2);
    std::thread::sleep(Duration::from_secs(1));
    queue.push(3);
    std::thread::sleep(Duration::from_secs(1));
    queue.push(4);
    std::thread::sleep(Duration::from_secs(1));
    queue.push(5);
    assert_eq!(queue.get_data(), vec![&1, &2, &3, &4, &5]);
    queue.retain();
    assert_eq!(queue.get_data(), vec![&1, &2, &3, &4, &5]);
    std::thread::sleep(Duration::from_millis(2100));
    queue.retain();
    assert_eq!(queue.get_data(), vec![&3, &4, &5]);
}