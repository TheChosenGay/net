

pub(crate) struct RingBuffer<T> {
    capacity: usize,
    buffer: Vec<Option<T>>,
    
    head: usize,
    tail: usize,
}

impl <T: Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {capacity, buffer: vec![None; capacity], head: 0, tail: 0}
    }
    
    pub fn push(&mut self, item: T) {
        if self.is_full() {
            self.head = (self.head + 1) % self.capacity;
        }
        
        self.buffer[self.tail] = Some(item);
        self.tail = (self.tail + 1) % self.capacity;

    }
    
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
           return None;
        }
        let ret =  self.buffer[self.head].take();
        self.head = (self.head + 1) % self.capacity;

        ret
    }
    
    pub fn size(&self) -> usize {
        return (self.tail + self.capacity - self.head) % self.capacity;
    }
    
    fn is_empty(&self) -> bool {
        self.head == self.tail
    }
    
    fn is_full(&self) -> bool {
        self.head == (self.tail + 1) % self.capacity
    }
    
}