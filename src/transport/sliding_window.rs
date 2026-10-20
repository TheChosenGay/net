use std::collections::BTreeSet;

/// `SlidingWindow::insert` 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlidingWindowInsertResult {
    /// 窗口内的新序号，已记录。
    New,
    /// 窗口内已经记录过的序号，重复包。
    Duplicate,
    /// 小于窗口下界的序号，已滑出窗口。
    TooOld,
}

/// 接收端去重滑动窗口。
///
/// 只解决一件事：判断一个键是新包、重复包还是已滑出窗口的旧包。
/// 不感知 Packet、ACK 和时间，键语义（单调递增、重传沿用原键）由调用方保证。
///
/// 窗口以键的高 64 位（sequence）为准：覆盖最近 `window_size` 个 sequence，
/// 低 64 位（fragment_id）只用于区分同一 sequence 下的不同分片。
/// 不能按 128 位数值直接算下界：sequence 每加 1，key 数值就跳 2^64，
/// 用数值差会把上一个 sequence 的所有分片瞬间误判为过期。
/// 窗口内允许缺号，以容纳乱序到达的包。
pub struct SlidingWindow {
    window_size: u64,
    received: BTreeSet<u128>,
    max_sequence: Option<u64>,
}

impl SlidingWindow {
    /// `window_size` 是窗口可覆盖的序号个数，必须大于 0。
    pub fn new(window_size: usize) -> Self {
        assert!(window_size > 0, "sliding window size must be greater than 0");
        Self {
            window_size: window_size as u64,
            received: BTreeSet::new(),
            max_sequence: None,
        }
    }

    /// 键的 sequence 是否已经小于窗口下界（无效旧包）。不改变窗口状态。
    pub fn is_invalid(&self, key: u128) -> bool {
        match self.max_sequence {
            None => false,
            Some(max) => Self::sequence_of(key) < self.window_low(max),
        }
    }

    /// 键是否已经在窗口中记录过。不改变窗口状态。
    pub fn is_received(&self, key: u128) -> bool {
        self.received.contains(&key)
    }

    /// 判断并记录一个键：
    /// 小于窗口下界返回 `TooOld`（不记录），窗口内已有返回 `Duplicate`（不重复记录），
    /// 否则记录并返回 `New`；若键超过当前最大值，窗口随之右移并淘汰滑出的旧键。
    pub fn insert(&mut self, key: u128) -> SlidingWindowInsertResult {
        if self.is_invalid(key) {
            return SlidingWindowInsertResult::TooOld;
        }
        if self.is_received(key) {
            return SlidingWindowInsertResult::Duplicate;
        }

        self.received.insert(key);

        let sequence = Self::sequence_of(key);
        let is_new_max = self.max_sequence.map_or(true, |max| sequence > max);
        if is_new_max {
            self.max_sequence = Some(sequence);
            let low = self.window_low(sequence);
            while let Some(&min) = self.received.iter().next() {
                if Self::sequence_of(min) >= low {
                    break;
                }
                self.received.pop_first();
            }
        }

        SlidingWindowInsertResult::New
    }

    fn sequence_of(key: u128) -> u64 {
        (key >> 64) as u64
    }

    fn window_low(&self, max_sequence: u64) -> u64 {
        max_sequence.saturating_sub(self.window_size - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::{SlidingWindow, SlidingWindowInsertResult::*};

    fn key(sequence: u64) -> u128 {
        (sequence as u128) << 64
    }

    #[test]
    fn new_then_duplicate() {
        let mut window = SlidingWindow::new(4);
        assert_eq!(window.insert(key(10)), New);
        assert_eq!(window.insert(key(10)), Duplicate);
        assert!(window.is_received(key(10)));
        assert!(!window.is_invalid(key(10)));
    }

    #[test]
    fn out_of_order_within_window_is_accepted() {
        let mut window = SlidingWindow::new(4);
        assert_eq!(window.insert(key(101)), New);
        // 101 先到时窗口为 [98, 101]，迟到的 100 是新包，不是重复包
        assert_eq!(window.insert(key(100)), New);
        assert_eq!(window.insert(key(100)), Duplicate);
    }

    #[test]
    fn sequence_below_window_is_invalid() {
        let mut window = SlidingWindow::new(4);
        assert_eq!(window.insert(key(10)), New);
        // 跳到 20 后窗口为 [17, 20]，10 已滑出
        assert_eq!(window.insert(key(20)), New);
        assert!(window.is_invalid(key(10)));
        assert!(!window.is_received(key(10)));
        assert_eq!(window.insert(key(10)), TooOld);
        // 窗口内的缺号仍然可以补收
        assert_eq!(window.insert(key(19)), New);
        assert_eq!(window.insert(key(16)), TooOld);
    }

    #[test]
    fn composite_u128_keys_are_distinct() {
        let mut window = SlidingWindow::new(4096);
        let key = |sequence: u128, fragment_id: u128| (sequence << 64) | fragment_id;
        assert_eq!(window.insert(key(10, 0)), New);
        assert_eq!(window.insert(key(10, 1)), New);
        assert_eq!(window.insert(key(10, 1)), Duplicate);
        assert_eq!(window.insert(key(10, 2)), New);
        // 下一个 sequence 到达后，上一个 sequence 的迟到分片在窗口内仍应被接受
        assert_eq!(window.insert(key(11, 0)), New);
        assert_eq!(window.insert(key(10, 3)), New);
    }

    #[test]
    fn window_size_one() {
        let mut window = SlidingWindow::new(1);
        assert_eq!(window.insert(key(5)), New);
        assert_eq!(window.insert(key(5)), Duplicate);
        assert_eq!(window.insert(key(6)), New);
        // 5 已被滑出，优先按 TooOld 处理
        assert_eq!(window.insert(key(5)), TooOld);
        assert_eq!(window.insert(key(4)), TooOld);
    }
}
