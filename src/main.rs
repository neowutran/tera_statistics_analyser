#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate serde_json;
extern crate glob;
extern crate threadpool;
extern crate chrono;
extern crate num_cpus;
mod parse;
mod process;
mod time_slice;
use parse::StatsLog;
use time_slice::TimeSlice;
use docopt::Docopt;
use std::{
  time::{SystemTime, UNIX_EPOCH},
  sync::mpsc::{Sender, Receiver},
  str,
  fs::File,
  path::Path,
  io::prelude::*,
  sync::mpsc,
  fs,
};
use glob::glob;
use threadpool::ThreadPool;
const USAGE: &'static str = "
Tera Statistics Analyser.

Usage:
  tera_statistics_analyser [--source <source>] [--target <target>] [--time-start <time_start>] [--time-steps <time_steps>] [--dps-steps <dps_steps>] [--dps-max <dps_max>]
  tera_statistics_analyser (-h | --help)
  tera_statistics_analyser --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --time-start <time_start>         Start date for export [default: 1519858800]
  --time-steps <time_steps>         Steps for time [default: 2629800]
  --dps-steps <dps_steps>           Steps for dps [default: 100000]
  --dps-max <dps_max>               Max plausible dps [default: 5000000]
  --source <source>                 Source data directory [default: ./].
  --target <target>                 Target data directory [default: ./].
";

const REGIONS: &'static [&'static str] = &["EU", "NA", "KR", "JP", "RU", "TW", "KR-PTS", "THA"];
#[derive(Debug, Deserialize)]
struct Args {
  flag_source: String,
  flag_target: String,
  flag_time_start: i64,
  flag_time_steps: i64,
  flag_dps_steps: i64,
  flag_dps_max: i64,
}


/// Find recursivly all .xz files from the source folder
/// Decompressed and parse them as Json
/// For each of those json, store statistics inside DB.
/// Export statistics to files
fn main() {
  let args: Args = Docopt::new(USAGE)
    .and_then(|d| d.deserialize())
    .unwrap_or_else(|e| e.exit());

  let start = SystemTime::now();
  let start: u64 = start.duration_since(UNIX_EPOCH).unwrap().as_secs();
  let time_slice = TimeSlice::new(args.flag_time_start, args.flag_time_steps);
  let (tx, rx): (Sender<Vec<StatsLog>>, Receiver<Vec<StatsLog>>) = mpsc::channel();
  let search = format!("{}/**/*.xz", args.flag_source);
  let full_cpus = num_cpus::get();
  let mut usable_cpus = full_cpus - 1;
  println!("Number of virtual core: {}", full_cpus);
  if usable_cpus <= 1{
    usable_cpus = 1;
  }
  let thread_pool_decompress: ThreadPool = ThreadPool::new(usable_cpus);
  for entry in glob(&search).expect("Failed to read glob pattern") {
    let os_string = entry.unwrap().into_os_string();
    let string = os_string.into_string().unwrap();
    let thread_tx = tx.clone();
    thread_pool_decompress.execute(move || {
      let contents = StatsLog::new(&string);
      thread_tx.send(contents).unwrap();
    });
  }

  drop(tx);
  let mut data = Box::new(process::GlobalData::new());
  for received in rx{
    data = process::store(&received, &time_slice, args.flag_dps_steps, data);
  }
  export(args.flag_target, &time_slice, args.flag_dps_max, args.flag_dps_steps, data);
  let end = SystemTime::now();
  let end: u64 = end.duration_since(UNIX_EPOCH).unwrap().as_secs();
  println!("duration: {} s", (end - start) as i64);
}

fn export(target: String, time_slice: &TimeSlice, dps_max: i64, dps_steps: i64, raw_data: Box<process::GlobalData> ){
  for (fight_key, mut fight_data) in raw_data.global{
    for region in REGIONS{
      for time in &time_slice.all_time {
        let key = process::get_key(region, time);
        let time_data = match fight_data.data.remove(&key){
          Some(t) => t,
          None => continue,
        };
        let result = process::export(time_data);
        let mut result_percentile_90 = String::new();
        let mut result_class = String::new();
        let mut result_median = String::new();
        for (class, data) in result.class{
          let mut result_dps = String::new();
          let mut dps = 0;
          while dps < dps_max{
            let count = match data.stepped_dps.get(&dps){
              Some(t) => t,
              None => &(0 as i64),
            };
            result_dps.push_str(&format!("{}:{}\n", dps, count));
            dps += dps_steps;
          }
          let filename = format!("{target}/dps/{area_boss}/{class}/{region}/{start}-{end}.txt", class = class, target = target, region = region, start = time.0, end = time.1, area_boss= fight_key.to_str());
          write_file(filename, &result_dps);
          result_percentile_90.push_str(&format!("{}:{}\n", class, data.percentile_90));
          result_class.push_str(&format!("{}:{}\n", class, data.count));
          result_median.push_str(&format!("{}:{}\n", class, data.median));
        }
        let end_filename = format!("/{area_boss}/{region}/{start}-{end}.txt", area_boss = fight_key.to_str() ,region = region, start = time.0, end = time.1);
        let filename = format!("{}/percentile_90/{}", target, end_filename);
        write_file(filename, &result_percentile_90);
        let filename = format!("{}/class/{}", target, end_filename);
        write_file(filename, &result_class);
        let filename = format!("{}/median/{}", target, end_filename);
        write_file(filename, &result_median);
      }
    }
  }
}

fn write_file(name: String, content: &String){
  let path = Path::new(&name);
  let parent = path.parent().unwrap();
  match fs::create_dir_all(parent){
    Ok(file) => file,
    Err(_) => {},
  }
  let display = path.display();
  let mut file = File::create(&path).expect(&format!("couldn't create {}", display));
  file.write_all(content.as_bytes()).expect(&format!("couldn't write to {}", display));
}
