#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate serde_json;
extern crate rusqlite;
extern crate glob;
extern crate threadpool;
extern crate chrono;
extern crate num_cpus;
pub mod parse;
pub mod process;
pub mod time_slice;
use parse::StatsLog;
use time_slice::TimeSlice;
use std::time::{SystemTime, UNIX_EPOCH};
use docopt::Docopt;
use std::sync::mpsc::{Sender, Receiver};
use std::str;
use std::fs::File;
use std::path::Path;
use std::io::prelude::*;
use glob::glob;
use std::sync::mpsc;
use threadpool::ThreadPool;
use std::collections::HashMap;
use std::fs;
use rusqlite::Connection;
use std::collections::HashSet;
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

/**
 * - Find recursivly all .xz files from the source folder
 * - Decompressed and parse them as Json
 * - For each of those json, store statistics inside DB.
 * - Export statistics to files
 **/
fn main() {
  let args: Args = Docopt::new(USAGE)
    .and_then(|d| d.deserialize())
    .unwrap_or_else(|e| e.exit());

  let start = SystemTime::now();
  let start: u64 = start.duration_since(UNIX_EPOCH).unwrap().as_secs();
  let conn = Connection::open_in_memory().unwrap();
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
  let mut area_boss = HashSet::new();
  for received in rx{
    process(&conn, &received, &time_slice, args.flag_dps_steps, &mut area_boss);
  }

  export(&conn, args.flag_target, &time_slice, args.flag_dps_max, args.flag_dps_steps, &area_boss);
  let end = SystemTime::now();
  let end: u64 = end.duration_since(UNIX_EPOCH).unwrap().as_secs();
  println!("duration: {} s", (end - start) as i64);
}

fn process(conn: &Connection, contents: &Vec<StatsLog>, time_slice: &TimeSlice, dps_steps: i64, area_boss: &mut HashSet<(i32,i32)>){
  process::store(conn, contents, time_slice, dps_steps, area_boss);
}

fn export(conn: &Connection, target: String, time_slice: &TimeSlice, dps_max: i64, dps_steps: i64, area_boss: &HashSet<(i32,i32)>){
  for time in &time_slice.all_time {
    for region in REGIONS{
      export_class(conn, &target, region, time.0, time.1, area_boss);
      export_dps(conn, &target, region, time.0, time.1, dps_max, dps_steps, area_boss);
      export_median_dps(conn, &target, region, time.0, time.1, area_boss);
      export_90_percentile_dps(conn, &target, region, time.0, time.1, area_boss);
    }
  }
}
fn export_90_percentile_dps(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64, area_boss: &HashSet<(i32, i32)>){
  for boss in area_boss{
    let area_id = boss.0;
    let boss_id = boss.1;
    let area_boss = format!("{}-{}", area_id, boss_id);
    let result = process::export_90_percentile_dps(conn, region, date_start, date_end, area_id, boss_id );
    let result = match result{
      Ok(r) => r,
      Err(e) => {println!("export 90 percentile {:?}",e); continue;},
    };
    let filename = format!("{target}/percentile_90/{area_boss}/{region}/{start}-{end}.txt", target = target, area_boss = area_boss ,region = region, start = date_start, end = date_end);
    write_file(filename, &result);
  }
}

fn export_median_dps(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64, area_boss: &HashSet<(i32, i32)>){
  for boss in area_boss{
    let area_id = boss.0;
    let boss_id = boss.1;
    let area_boss = format!("{}-{}", area_id, boss_id);
    let result = process::export_median_dps(conn, region, date_start, date_end, area_id, boss_id );
    let result = match result{
      Ok(r) => r,
      Err(e) => {println!("export median {:?}",e); continue;},
    };
    let filename = format!("{target}/median/{area_boss}/{region}/{start}-{end}.txt", target = target, area_boss = area_boss ,region = region, start = date_start, end = date_end);
    write_file(filename, &result);
  }
}
fn export_dps(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64, dps_max: i64, dps_steps: i64, area_boss: &HashSet<(i32, i32)>){
  for boss in area_boss{
    let area_id = boss.0;
    let boss_id = boss.1;
    let area_boss = format!("{}-{}", area_id, boss_id);
    let results = process::export_dps(conn, region, date_start, date_end, dps_max, area_id, boss_id, dps_steps );
    let results = match results{
      Ok(r) => r,
      Err(e) => {println!("export dps {:?}", e); continue;},
    };
    for result in results{
      let class = result.0;
      let data = result.1;
      let filename = format!("{target}/dps/{area_boss}/{class}/{region}/{start}-{end}.txt", target = target, area_boss = area_boss, class = class, region = region, start = date_start, end = date_end);
      write_file(filename, &data);
    }
  }
}
fn export_class(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64, area_boss: &HashSet<(i32,i32)>){
  let mut global_result = HashMap::new();
  for boss in area_boss{
    let area_id = boss.0;
    let boss_id = boss.1;
    let area_boss = format!("{}-{}", area_id, boss_id);
    let results = process::export_class(conn, region, date_start, date_end, area_id, boss_id );
    let results = match results{
      Ok(o) => o,
      Err(e) => {println!("export class {:?}",e);continue;},
    };
    let mut lines= String::new();
    for result in results {
      let class = result.0;
      let count = result.1;
      let class_copy = class.clone();
      global_result.entry(class_copy).or_insert(0 as i64);
      let stat = count + global_result.get(&class).unwrap();
      let line = format!("{}:{}\n", &class, count);
      global_result.insert(class, stat);
      lines.push_str(&line);
    }
    let filename = format!("{target}/class/{area_boss}/{region}/{start}-{end}.txt", target = target, region = region, start = date_start, end = date_end, area_boss= area_boss);
    write_file(filename, &lines);

  }
  let mut result = String::new();
  for d in global_result{
    let line = format!("{}:{}\n", d.0, d.1);
    result.push_str(&line);
  }
  let filename = format!("{target}/class/{region}/{start}-{end}.txt", target = target, region = region, start = date_start, end = date_end);
  write_file(filename, &result);
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
