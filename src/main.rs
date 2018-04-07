#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate docopt;
extern crate glob;
extern crate num_cpus;
extern crate serde_json;
extern crate threadpool;

#[macro_use]
mod bidir_map;
mod parse;
mod process;
mod time_slice;
use bidir_map::BidirMap;
use docopt::Docopt;
use glob::glob;
use parse::StatsLog;
use process::Class;
use std::{fs, str, collections::HashMap, fs::File, io::prelude::*, path::Path, sync::mpsc,
          sync::mpsc::{Receiver, Sender}, time::{SystemTime, UNIX_EPOCH}};
use threadpool::ThreadPool;
use time_slice::TimeSlice;
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

#[derive(Deserialize)]
struct Args {
    flag_source: String,
    flag_target: String,
    flag_time_start: u64,
    flag_time_steps: u32,
    flag_dps_steps: u32,
    flag_dps_max: u32,
}

macro_rules! hashmap {
  (@single $($x:tt)*) => (());
  (@count $($rest:expr),*) => (<[()]>::len(&[$(hashmap!(@single $rest)),*]));
  ($( $key: expr => $val: expr ),*) => {{
    let cap = hashmap!(@count $($key), *);
    let mut map = ::std::collections::HashMap::with_capacity(cap);
    $( map.insert($key, $val); )*
      map
  }}
}
/// Find recursivly all .xz files from the source folder
/// Decompressed and parse them as Json
/// For each of those json, store statistics inside DB.
/// Export statistics to files
fn main() {
    let start = SystemTime::now();
    let start: u64 = start.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    let region_map = hashmap![
    "EU" => hashmap!["63.0" => 1234542521],
    "NA" => hashmap![],
    "KR" => hashmap![],
    "JP" => hashmap![],
    "RU" => hashmap![],
    "TW" => hashmap![],
    "THA" => hashmap![]
  ];
    let class_map = bidir_map!(
      "Archer" => Class::Archer,
      "Berserker" => Class::Berserker,
      "Brawler" => Class::Brawler,
      "Gunner" => Class::Gunner,
      "Lancer" => Class::Lancer,
      "Mystic" => Class::Mystic,
      "Ninja" => Class::Ninja,
      "Priest" => Class::Priest,
      "Reaper" => Class::Reaper,
      "Slayer" => Class::Slayer,
      "Sorcerer" => Class::Sorcerer,
      "Valkyrie" => Class::Valkyrie,
      "Warrior" => Class::Warrior,
      );

    let time_slice = TimeSlice::new(args.flag_time_start, args.flag_time_steps);
    let (tx, rx): (Sender<Vec<StatsLog>>, Receiver<Vec<StatsLog>>) = mpsc::channel();
    let search = format!("{}/**/*.xz", args.flag_source);
    let full_cpus = num_cpus::get();
    let mut usable_cpus = full_cpus - 1;
    println!("Number of virtual core: {}", full_cpus);
    if usable_cpus <= 1 {
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
    let mut data = process::GlobalData::new();
    for received in rx {
        data = process::store(received, &time_slice, args.flag_dps_steps, data, &class_map);
    }
    export(
        args.flag_target,
        &time_slice,
        args.flag_dps_max,
        args.flag_dps_steps,
        data,
        &class_map,
        &region_map,
    );
    let end = SystemTime::now();
    let end: u64 = end.duration_since(UNIX_EPOCH).unwrap().as_secs();
    println!("duration: {} s", (end - start) as i64);
}

fn export(
    target: String,
    time_slice: &TimeSlice,
    dps_max: u32,
    dps_steps: u32,
    raw_data: process::GlobalData,
    class_map: &BidirMap<&str, Class>,
    region_map: &HashMap<&str, HashMap<&str, u64>>,
) {
    let mut class_global = HashMap::new();
    for (fight_key, mut fight_data) in raw_data {
        for region in region_map.keys() {
            for time in &time_slice.all_time {
                let key = process::get_key(region, time);
                let time_data = match fight_data.remove(&key) {
                    Some(t) => t,
                    None => continue,
                };
                let result = process::export(time_data, class_map);
                let mut result_percentile_90 = String::new();
                let mut result_class = String::new();
                let mut result_median = String::new();
                let mut result_healers_number = String::new();
                for (healers_number, count) in result.healers_number {
                    result_healers_number.push_str(&format!("{}:{}\n", healers_number, count));
                }
                write_file(
                    format!(
                        "{target}/healers/{area_boss}/{region}/{start}-{end}.txt",
                        target = target,
                        region = region,
                        start = time.0,
                        end = time.1,
                        area_boss = fight_key.to_str()
                    ),
                    &result_healers_number,
                );
                write_file(
                    format!(
                        "{target}/clear_time/{area_boss}/{region}/{start}-{end}.txt",
                        target = target,
                        region = region,
                        start = time.0,
                        end = time.1,
                        area_boss = fight_key.to_str()
                    ),
                    &format!(
                        "{};{}",
                        result.clear_time_median, result.clear_time_percentile_90
                    ),
                );
                for (class, data) in result.class {
                    let mut result_dps = String::new();
                    let class = class_map.get_by_second(&class).unwrap();
                    let mut dps = 0;
                    while dps < dps_max {
                        let count = match data.stepped_dps.get(&dps) {
                            Some(t) => t,
                            None => &(0 as u32),
                        };
                        result_dps.push_str(&format!("{}:{}\n", dps, count));
                        dps += dps_steps;
                    }
                    write_file(
                        format!(
                            "{target}/dps/{area_boss}/{class}/{region}/{start}-{end}.txt",
                            class = class,
                            target = target,
                            region = region,
                            start = time.0,
                            end = time.1,
                            area_boss = fight_key.to_str()
                        ),
                        &result_dps,
                    );
                    result_percentile_90
                        .push_str(&format!("{}:{}\n", class, data.dps_percentile_90));
                    result_class.push_str(&format!("{}:{}\n", class, data.count));
                    *(class_global
                        .entry(region)
                        .or_insert(HashMap::new())
                        .entry(format!("{}-{}", time.0, time.1))
                        .or_insert(HashMap::new())
                        .entry(class.clone())
                        .or_insert(0)) += data.count;
                    result_median.push_str(&format!("{}:{}\n", class, data.dps_median));
                }
                let end_filename = format!(
                    "/{area_boss}/{region}/{start}-{end}.txt",
                    area_boss = fight_key.to_str(),
                    region = region,
                    start = time.0,
                    end = time.1
                );
                write_file(
                    format!("{}/dps_percentile_90/{}", target, end_filename),
                    &result_percentile_90,
                );
                write_file(format!("{}/class/{}", target, end_filename), &result_class);
                write_file(
                    format!("{}/dps_median/{}", target, end_filename),
                    &result_median,
                );
            }
        }
    }

    for region in region_map.keys() {
        let class_global_region = match class_global.get(region) {
            Some(t) => t,
            None => continue,
        };
        for time in &time_slice.all_time {
            let class_global_region_time =
                match class_global_region.get(&format!("{}-{}", time.0, time.1)) {
                    Some(t) => t,
                    None => continue,
                };
            let mut global_class_str = String::new();
            for (class, count) in class_global_region_time {
                global_class_str.push_str(&format!("{}:{}\n", class, count));
            }
            write_file(
                format!("{}/class/{}/{}-{}.txt", target, region, time.0, time.1),
                &global_class_str,
            );
        }
    }
}

fn write_file(name: String, content: &String) {
    let path = Path::new(&name);
    let parent = path.parent().unwrap();
    match fs::create_dir_all(parent) {
        Ok(file) => file,
        Err(_) => {}
    }
    let display = path.display();
    let mut file = File::create(&path).expect(&format!("couldn't create {}", display));
    file.write_all(content.as_bytes())
        .expect(&format!("couldn't write to {}", display));
}
