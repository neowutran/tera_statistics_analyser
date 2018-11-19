extern crate chrono;
extern crate docopt;
extern crate glob;
extern crate num_cpus;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate threadpool;

#[macro_use]
mod bidir_map;
mod parse;
mod process;
use bidir_map::BidirMap;
use docopt::Docopt;
use glob::glob;
use parse::StatsLog;
use process::Class;
use std::{
    collections::HashMap, fs, fs::File, io::prelude::*, path::Path, str, sync::mpsc,
    sync::mpsc::{Receiver, Sender}, time::{SystemTime, UNIX_EPOCH},
};
use threadpool::ThreadPool;
const USAGE: &'static str = "
Tera Statistics Analyser.

Usage:
  tera_statistics_analyser <source> <target> [--dps-steps <dps_steps>] [--dps-max <dps_max>]
  tera_statistics_analyser (-h | --help)

Options:
  -h --help                         Show this screen.
  --dps-steps <dps_steps>           Steps for dps [default: 100000]
  --dps-max <dps_max>               Max plausible dps [default: 10000000]
";

#[derive(Deserialize)]
struct Args {
    arg_source: String,
    arg_target: String,
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

fn main() {
    let start = SystemTime::now();
    let start: u64 = start.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    let region_map = hashmap![
    "EU" => hashmap![
      //"66" => (1520330400,1523354400),
      //"67" => (1523354400,1526378400),
      //"68"=> (1526378400, 1528365600),
      //"69"=> (1528365600, 1531390284),
      //"71" => ( 1531390284,1536770959),
      "old" => (u64::min_value(), 1542104390),
      //"74" => (1536770959,u64::max_value()),
      //"75" => (1539073061, u64::max_value())
      "76" => (1542104390, u64::max_value())
    ],
    "NA" => hashmap![

      "old" => (u64::min_value(), 1542104390),
      //"66" => (1520964000, 1523988000),
      //"67"=>(1523988000, 1526378400),
      //"68"=>(1526378400, 1528365600),
      //"69" => (1528365600, 1531390284),
      //"71" => (1531390284, 1536770959),
      //"74" => (1536770959,u64::max_value())
      "76" => (1542104390,u64::max_value())
    ],
    "KR" => hashmap![

      "old" => (u64::min_value(), 1541075667),
      //"69" => (1515024000, 1522281600),
      //"71"=> (1522281600, 1530025242),
      //"74"=> (1530025242, 1531389600),
      //"75" => (1531389600, 1533163873),
      //"76" => (1533163873, 1536770959),
      //"77" => (1536770959,u64::max_value())
      "79" => (1541075667,u64::max_value())
    ],
    "JP" => hashmap![

      "old" => (u64::min_value(), 1542104390),
      //"66" => (1520380800, 1523434273),
      //"67" => (1523434273, 1525824000),
      //"68" => (1525824000, 1528279200),
      //"69" => (1528279200, 1531303884),
      //"71" => (1531303884, 1536770959),
      //"74" => (1536770959, u64::max_value()),
      //"75" => (1539159461, u64::max_value())
      "76" => (1542104390, u64::max_value())
    ],
    "RU" => hashmap![

      "old" => (u64::min_value(), 1539159461),
      //"66" => (1520899200, 1524009600),
      //"67" => (1524009600, 1526378400),
      //"68" => (1526378400, 1528279200),
      //"69" => (1528279200, 1531303884),
      //"71" => (1531303884, 1536770959),
      //"74"=> (1536770959,u64::max_value())

      "75" => (1539159461, u64::max_value())
    ],
    "THA" => hashmap![

      "old" => (u64::min_value(),1539952467),
      //"66" => (u64::min_value(), 1522886400),
      //"67" => (1522886400, 1525824000),
      //"68"=>(1525824000, 1528279200),
      //"69" => (1528279200, 1531303884),
      //"71" => (1531303884, 1536770959),
      //"74"=> (1536770959, u64::max_value())
      "75"=> (1539952467, u64::max_value())
    ],
    "TW" => hashmap![

      "old" => (u64::min_value(), 1542104390),
      //"66"=> (1521417600, 1523520673),
      //"67"=>(1523520673, 1525824000),
      //"68" => ( 1525824000, 1528279200),
      //"69" => (1528279200, 1531303884),
      //"71" => (1531303884,1536770959),
      //"74" => (1536770959,u64::max_value())
      //"75" => (1539952467,u64::max_value())
      "76" => (1542104390, u64::max_value())
    ]
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

    let (tx, rx): (Sender<Vec<StatsLog>>, Receiver<Vec<StatsLog>>) = mpsc::channel();
    let search = format!("{}/**/*.xz", args.arg_source);
    let full_cpus = num_cpus::get();
    let mut usable_cpus = full_cpus - 3;
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
            match StatsLog::new(&string) {
                Ok(data) => thread_tx.send(data).unwrap(),
                Err(err) => {
                    println!("{}", err);
                    fs::remove_file(&string).unwrap();
                }
            };
        });
    }

    drop(tx);
    let mut global_data = process::GlobalData::new();
    for received in rx {
        process::store(
            received,
            args.flag_dps_steps,
            &mut global_data,
            &class_map,
            &region_map,
        );
    }
    export(
        args.arg_target,
        args.flag_dps_max,
        args.flag_dps_steps,
        global_data,
        &class_map,
        &region_map,
    );
    let end = SystemTime::now();
    let end: u64 = end.duration_since(UNIX_EPOCH).unwrap().as_secs();
    println!("duration: {} s", (end - start) as i64);
}

fn export(
    target: String,
    dps_max: u32,
    dps_steps: u32,
    raw_data: process::GlobalData,
    class_map: &BidirMap<&str, Class>,
    region_map: &HashMap<&str, HashMap<&str, (u64, u64)>>,
) {
  /*
    for (date, mut region) in raw_data.usage{
      let mut result_usage = String::new();
      for(region, number) in region{
        result_usage.push_str(&format!("{}:{}\n", region, number));
      }
      write_file(
                        format!(
                            "{target}/usage/{date}.txt",
                            target = target,
                            date = date,
                        ),
                        &result_usage,
                    );

    }*/
    let mut class_global = HashMap::new();
    for (fight_key, mut fight_data) in raw_data.fights {
        for (region, patch_data) in region_map {
            for patch_name in patch_data.keys() {
                let key = process::get_key(region, patch_name);
                let time_data = match fight_data.remove(&key) {
                    Some(t) => t,
                    None => continue,
                };
                let result = process::export(time_data, class_map);
                /*
                {
                    let mut result_healers_number = String::new();
                    for (healers_number, count) in result.healers_number {
                        result_healers_number.push_str(&format!("{}:{}\n", healers_number, count));
                    }
                    write_file(
                        format!(
                            "{target}/healers/{area_boss}/{region}/{patch_name}.txt",
                            target = target,
                            region = region,
                            patch_name = patch_name,
                            area_boss = fight_key.to_str()
                        ),
                        &result_healers_number,
                    );
                }*/
                write_file(
                    format!(
                        "{target}/clear_time/{area_boss}/{region}/{patch_name}.txt",
                        target = target,
                        region = region,
                        patch_name = patch_name,
                        area_boss = fight_key.to_str()
                    ),
                    &format!(
                        "{};{}",
                        result.clear_time_median, result.clear_time_percentile_90
                    ),
                );
                let mut result_percentile_90 = String::new();
                let mut result_class = String::new();
                let mut result_median = String::new();
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
                            "{target}/dps/{area_boss}/{class}/{region}/{patch_name}.txt",
                            class = class,
                            target = target,
                            region = region,
                            patch_name = patch_name,
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
                        .entry(patch_name)
                        .or_insert(HashMap::new())
                        .entry(class.clone())
                        .or_insert(0)) += data.count;
                    result_median.push_str(&format!("{}:{}\n", class, data.dps_median));
                }
                let end_filename = format!(
                    "/{area_boss}/{region}/{patch_name}.txt",
                    area_boss = fight_key.to_str(),
                    region = region,
                    patch_name = patch_name,
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

    for (region, patch_data) in region_map {
        let class_global_region = match class_global.get(region) {
            Some(t) => t,
            None => continue,
        };
        for patch_name in patch_data.keys() {
            let class_global_region_time = match class_global_region.get(patch_name) {
                Some(t) => t,
                None => continue,
            };
            let mut global_class_str = String::new();
            for (class, count) in class_global_region_time {
                global_class_str.push_str(&format!("{}:{}\n", class, count));
            }
            write_file(
                format!("{}/class/{}/{}.txt", target, region, patch_name),
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
