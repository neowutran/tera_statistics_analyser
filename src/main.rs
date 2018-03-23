#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate serde_json;
extern crate rusqlite;
extern crate glob;
extern crate threadpool;
extern crate chrono;
extern crate num_cpus;
use std::time::{SystemTime, UNIX_EPOCH};
use docopt::Docopt;
use std::sync::mpsc::{Sender, Receiver};
use std::str;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::io::prelude::*;
use glob::glob;
use std::sync::mpsc;
use threadpool::ThreadPool;
use std::fs;
use rusqlite::Connection;
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
  initialize(&conn);

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
  for received in rx{
    process(&conn, &received);
  }

  export(&conn, args.flag_target, args.flag_time_start, args.flag_time_steps, args.flag_dps_steps, args.flag_dps_max);
  let end = SystemTime::now();
  let end: u64 = end.duration_since(UNIX_EPOCH).unwrap().as_secs();
  println!("duration: {} s", (end - start) as i64);
}

fn initialize(conn: &Connection){
  count::initialize(conn);
}

fn process(conn: &Connection, contents: &Vec<StatsLog>){
  count::process(conn, contents);
}

fn export(conn: &Connection, target: String, time_start: i64, time_step: i64, dps_steps: i64, dps_max: i64){
  let mut beginning = time_start;
  let current = SystemTime::now();
  let current = current.duration_since(UNIX_EPOCH).unwrap();
  let current = current.as_secs() as i64;
  while beginning < current{
    let end = beginning + time_step;
    for region in REGIONS{
      export_class(conn, &target, region, beginning, end);
      export_dps(conn, &target, region, beginning, end, dps_steps, dps_max);
      export_median_dps(conn, &target, region, beginning, end);
    }
    beginning = end;
  }
}

fn export_median_dps(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64){
  let all_boss = count::get_all_boss(conn);
  for boss in all_boss{
    let area_id = boss.0;
    let boss_id = boss.1;
    let area_boss = format!("{}-{}", area_id, boss_id);
    let result = count::export_median_dps(conn, region, date_start, date_end, area_id, boss_id );
    let filename = format!("{target}/median/{area_boss}/{region}/{start}-{end}.txt", target = target, area_boss = area_boss ,region = region, start = date_start, end = date_end);
    write_file(filename, &result);
  }
}
fn export_dps(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64, dps_steps: i64, dps_max: i64){
  let results = count::export_dps(conn, region, date_start, date_end, dps_steps, dps_max );
  for result in results{
    let key = result.0;
    let boss_id = key.0;
    let dungeon_id = key.1;
    let class = key.2;
    let data = result.1;
    let area_boss = format!("{}-{}", dungeon_id, boss_id);
    let filename = format!("{target}/dps/{area_boss}/{class}/{region}/{start}-{end}.txt", target = target, area_boss = area_boss, class = class, region = region, start = date_start, end = date_end);
    write_file(filename, &data);
  }
}
fn export_class(conn: &Connection, target: &String, region: &str, date_start: i64, date_end: i64){
  let result = count::export_class(conn, region, date_start, date_end );
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
  let mut file = match File::create(&path) {
    Err(why) => panic!("couldn't create {}: {}", display, why),
    Ok(file) => file,
  };
  match file.write_all(content.as_bytes()) {
    Err(why) => {
      panic!("couldn't write to {}: {}", display, why)
    },
    Ok(_) => {},
  }
}

mod count{
  use StatsLog;
  use rusqlite::Rows;
  use rusqlite::Error;
  use std::collections::HashMap;
  use rusqlite::Connection;

  pub const CLASS: &'static [&'static str] = &["Archer","Berserker","Brawler","Gunner","Lancer","Mystic","Ninja","Priest","Reaper","Slayer","Sorcerer","Valkyrie","Warrior"];
  pub fn initialize(conn: &Connection){
    conn.execute("CREATE TABLE data (
                  id              INTEGER PRIMARY KEY,
                  dps             INTEGER NOT NULL,
                  class_name      TEXT NOT NULL,
                  region          TEXT NOT NULL,
                  area_id         INTEGER NOT NULL,
                  boss_id         INTEGER NOT NULL,
                  time            INTEGER NOT NULL
                  )", &[]).unwrap();
  }

  pub fn process(conn: &Connection, contents: &Vec<StatsLog>) {
    for content in contents{
      let directory_split = content.directory.split(".");
      let directory_vec: Vec<&str> = directory_split.collect();
      let region = String::from(directory_vec[0]);
      let timestamp = content.content.timestamp;
      let dungeon:i32 = content.content.area_id.parse().unwrap();
      let boss:i32 = content.content.boss_id.parse().unwrap();
      for member in &content.content.members{
        let class = &member.player_class;
        let found = CLASS.iter().find(|&&c| c == class);
        match found{
          Some(_) => {
            let dps: i64 = member.player_dps.parse().unwrap();
            conn.execute_named("INSERT INTO data (dps, class_name, region, area_id, boss_id, time)
                  VALUES (:dps, :class_name, :region, :area_id, :boss_id, :time)", &[(":dps", &dps),(":class_name", class), (":region",&region), (":area_id",&dungeon), (":boss_id",&boss), (":time",&timestamp)]).unwrap();
          }
          None => {}
        };
      }
    }
  }

  fn parse_sql_result_dps(rows: Result<Rows, Error>, dps_steps: i64) -> HashMap<(i32, i32, String), HashMap<i64, i64>>{
    let mut rows = rows.unwrap();
    let mut data = HashMap::new();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let count: i64 = row.get(0);
      let class: String = row.get(1);
      let dps: i64 = row.get(2);
      let dps = ((dps / dps_steps) as i64) * dps_steps;
      let boss_id: i32 = row.get(3);
      let area_id: i32 = row.get(4);
      let key = (boss_id, area_id, class);
      let stat = data.entry(key).or_insert(HashMap::new());
      stat.insert(dps, count);
    }
    data
  }

  pub fn export_class(conn: &Connection, region: &str, date_start: i64, date_end: i64)-> String{
    let mut stmt = conn.prepare("SELECT count(1), class_name from data where region = :region and time >= :date_start and time <= :date_end group by class_name").unwrap();
    let rows = stmt.query_named(&[(":region", &region), (":date_start", &date_start), (":date_end", &date_end)]);
    let mut rows = rows.unwrap();
    let mut result = String::new();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let count: i64 = row.get(0);
      let name: String = row.get(1);
      let line = format!("{}:{}\n", name, count);
      result.push_str(&line);
    }
    result
  }

  pub fn export_dps(conn: &Connection, region: &str, date_start: i64, date_end: i64, dps_steps: i64, dps_max: i64)-> HashMap<(i32, i32, String), String>{
    let mut stmt = conn.prepare("SELECT count(1), class_name, dps, boss_id, area_id from data where region = :region and time >= :date_start and time <= :date_end group by class_name, dps, boss_id, area_id").unwrap();
    let rows = stmt.query_named(&[(":region", &region), (":date_start", &date_start), (":date_end", &date_end)]);
    let mut final_result = HashMap::new();
    let results = parse_sql_result_dps(rows, dps_steps);
    for result in results{
      let key = result.0;
      let data = result.1;
      let mut data_str = String::new();
      let mut dps = 0;
      while dps <= dps_max {
        let mut count: i64 = 0;
        if data.contains_key(&dps) {
          count += data.get(&dps).unwrap();
        }
        data_str.push_str(&format!("{}:{}\n", dps, count));
        dps += dps_steps;
      }
      final_result.insert(key, data_str);
    }
    final_result
  }


  pub fn export_median_dps(conn: &Connection, region: &str, date_start: i64, date_end: i64, area_id: i32, boss_id: i32)->String{
    let mut result = String::new();
    for c in CLASS{
     conn.query_row_named("SELECT AVG(dps) FROM (
      SELECT dps FROM data WHERE region = :region and time >= :start and time <= :end and class_name = :class and area_id = :area_id and boss_id = :boss_id ORDER BY dps LIMIT 2 - (
      SELECT COUNT(*) FROM data where region = :region and time >= :start and time <= :end and class_name = :class and area_id = :area_id and boss_id = :boss_id) % 2 OFFSET (
      SELECT (COUNT(*) - 1) / 2 FROM data where region = :region and time >= :start and time <= :end and class_name = :class and area_id = :area_id and boss_id = :boss_id))
        ", &[(":region",&region), (":start",&date_start), (":end",&date_end),(":class",c), (":area_id",&area_id), (":boss_id",&boss_id)], |row| {
        let median: Option<f64> = row.get(0);
        if median.is_some(){
        let line = format!("{}:{}\n", c, median.unwrap());
         result.push_str(&line);
        }
    }).unwrap();
    }
    result
  }

  pub fn get_all_boss(conn: &Connection)->Vec<(i32,i32)>{
    let mut result = Vec::new();
    let mut stmt = conn.prepare("SELECT distinct area_id, boss_id from data ").unwrap();
    let rows = stmt.query_named(&[]);
    let mut rows = rows.unwrap();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let area_id: i32 = row.get(0);
      let boss_id: i32 = row.get(1);
      result.push((area_id, boss_id));
    }
    result
  }
}
impl StatsLog{
  pub fn new(filename: &String) -> Vec<StatsLog>{
    // Rust-lzma crash on magic byte detection for XZ. So back to system version until I found why.
    // To improve and find why rust-lzma doesn't work.
    let mut command = String::from("unxz --stdout ");
    command.push_str(&filename);
    let output = Command::new("sh")
      .arg("-c")
      .arg(command)
      .output()
      .expect("failed to execute process");
    let stdout = String::from_utf8(output.stdout).unwrap();
    serde_json::from_str(&stdout).unwrap()
  }
}

// Full json structure
#[derive(Deserialize)]
pub struct StatsLog {
  content: Encounter,
  directory: String,
  //name: String,
}

#[derive(Deserialize)]
struct Encounter {
  #[serde(rename="areaId")]
  area_id: String,
  #[serde(rename="bossId")]
  boss_id: String,
  //#[serde(rename="debuffDetail")]
  //debuff_detail: Vec<Vec<Value>>,
  //#[serde(rename="debuffUptime")]
  //debuff_uptime: Vec<Value>,
  //#[serde(rename="encounterUnixEpoch")]
  //encounter_unix_epoch: i64,
  //#[serde(rename="fightDuration")]
  //fight_duration: String,
  timestamp: i64,
  members: Vec<Members>,
  //#[serde(rename="meterName")]
  //meter_name: String,
  //#[serde(rename="meterVersion")]
  //meter_version: String,
  //#[serde(rename="partyDps")]
  //party_dps: String,
  //#[serde(default)]
  //uploader: String, //zero-based index of uploader in members list
}


#[derive(Deserialize)]
struct Members{
  //aggro: String,
  //#[serde(rename="buffDetail")]
  //buff_detail: Vec<Value>,
  //#[serde(rename="buffUptime")]
  //buff_uptime: Vec<Value>,
  //#[serde(default)]
  //guild: String,
  //#[serde(default)]
  //#[serde(rename="healCrit")]
  //heal_crit: String,
  //#[serde(rename="playerAverageCritRate")]
  //player_average_crit_rate: String,
  #[serde(rename="playerClass")]
  player_class: String,
  //#[serde(rename="playerDeathDuration")]
  //player_death_duration: String,
  //#[serde(rename="playerDeaths")]
  //player_deaths: String,
  #[serde(rename="playerDps")]
  player_dps: String,
  //#[serde(rename="playerId")]
  //player_id:u32,
  //#[serde(rename="playerName")]
  //player_name: String,
  //#[serde(rename="playerServer")]
  //player_server: String,
  //#[serde(rename="playerTotalDamage")]
  //player_total_damage: String,
  //#[serde(rename="playerTotalDamagePercentage")]
  //player_total_damage_percentage: String,
  //#[serde(rename="skillLog")]
  //skill_log: Vec<SkillLog>,
  //#[serde(rename="skillCasts")]
  //skill_casts: Vec<Vec<i32>>,
}

#[derive(Deserialize)]
struct SkillLog{
  //#[serde(rename="skillAverageCrit")]
  //skill_average_crit: String,
  //#[serde(rename="skillAverageWhite")]
  //skill_average_white: String,
  //#[serde(rename="skillCritRate")]
  //skill_crit_rate: String,
  //#[serde(rename="skillDamagePercent")]
  //skill_damage_percent: String,
  //#[serde(rename="skillHighestCrit")]
  //skill_highest_crit: String,
  //#[serde(rename="skillHits")]
  //skill_hits: String,
  //#[serde(rename="skillId")]
  //skill_id: String,
  //#[serde(rename="skillLowestCrit")]
  //skill_lowest_crit: String,
  //#[serde(rename="skillTotalDamage")]
  //skill_total_damage: String,
}
