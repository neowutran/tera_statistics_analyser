
use std::time::{SystemTime, UNIX_EPOCH};
pub struct TimeSlice{
  pub all_time: Vec<(i64,i64)>,
}

impl TimeSlice{
  pub fn get_time_slice(&self,time: i64) -> Option<(i64,i64)>{
    for t in &self.all_time {
      if time >= t.0 && time <= t.1{
        return Some((t.0, t.1));
      }
    }
    None
  }
  pub fn new(time_start: i64, time_step: i64)-> TimeSlice{
    let mut result = Vec::new();
    let mut beginning = time_start;
    let current = SystemTime::now();
    let current = current.duration_since(UNIX_EPOCH).unwrap();
    let current = current.as_secs() as i64;
    while beginning < current{
      let end = beginning + time_step;
      result.push((beginning, end));
      beginning = end;
    }
    TimeSlice{all_time: result}
  }
}


