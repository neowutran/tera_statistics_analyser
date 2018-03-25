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


pub struct TimeSlice{
  all_time: Vec<(i64,i64)>,
}

impl TimeSlice{
  pub fn get_time_slice(&self,time: i64) -> Option<(i64,i64)>{
    for t in &self.all_time {
      if time >= t.0 && time <= t.1{
        let res: Option<(i64,i64)> = Some((t.0, t.1));
        return res;
      }
    }
    None
  }
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
  let time_slice = get_time_slice(args.flag_time_start, args.flag_time_steps);
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

fn get_time_slice(time_start: i64, time_step: i64)-> TimeSlice{
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

fn process(conn: &Connection, contents: &Vec<StatsLog>, time_slice: &TimeSlice, dps_steps: i64, area_boss: &mut HashSet<(i32,i32)>){
  count::process(conn, contents, time_slice, dps_steps, area_boss);
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
    let result = count::export_90_percentile_dps(conn, region, date_start, date_end, area_id, boss_id );
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
    let result = count::export_median_dps(conn, region, date_start, date_end, area_id, boss_id );
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
    let results = count::export_dps(conn, region, date_start, date_end, dps_max, area_id, boss_id, dps_steps );
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
    let results = count::export_class(conn, region, date_start, date_end, area_id, boss_id );
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
  use TimeSlice;
  use rusqlite::Rows;
  use rusqlite::Error;
  use std::collections::HashMap;
  use rusqlite::Connection;
  use std::collections::HashSet;
  pub const CLASS: &'static [&'static str] = &["Archer","Berserker","Brawler","Gunner","Lancer","Mystic","Ninja","Priest","Reaper","Slayer","Sorcerer","Valkyrie","Warrior"];

  #[derive(Debug)]
  pub enum CountError{
    NoData
  }

  fn get_table_name(area: i32, boss: i32, region: &str, start: i64, end: i64) -> String{
     let region = region.replace("-","_");
    format!("{region}_{area}_{boss}_{start}_{end}", boss=boss, area = area, region = region, start = start, end = end)
  }

  pub fn process(conn: &Connection, contents: &Vec<StatsLog>, time_slice: &TimeSlice, dps_steps: i64, area_boss: &mut HashSet<(i32, i32)>) {
    for content in contents{
      let directory_split = content.directory.split(".");
      let directory_vec: Vec<&str> = directory_split.collect();
      let region = String::from(directory_vec[0]);
      let timestamp = content.content.timestamp;
      let dungeon:i32 = content.content.area_id.parse().unwrap();
      let boss:i32 = content.content.boss_id.parse().unwrap();
      area_boss.insert((dungeon,boss));
      let time = time_slice.get_time_slice(timestamp);
      if !time.is_some(){
        continue;
      }
      let time = time.unwrap();
      let table_name = get_table_name(dungeon, boss, &region, time.0, time.1);
      let sql = "CREATE TABLE IF NOT EXISTS {} (
              id        INTEGER PRIMARY KEY,
              dps       INTEGER NOT NULL,
              dps_stepped INTEGER NOT NULL,
              class_name TEXT NOT NULL
          )";
      let create_table = sql.replace("{}",&table_name);
      conn.execute_named(&create_table, &[]).unwrap();

      for member in &content.content.members{
        let class = &member.player_class;
        let found = CLASS.iter().find(|&&c| c == class);
        match found{
          Some(_) => {
            let dps: i64 = member.player_dps.parse().unwrap();
            let stepped_dps = ((dps / dps_steps) as i64) * dps_steps;
            let sql = "INSERT INTO {} (dps, dps_stepped, class_name) VALUES (:dps, :dps_stepped ,:class_name)";
            let insert = sql.replace("{}",&table_name);
            conn.execute_named(&insert, &[(":dps", &dps), (":dps_stepped", &stepped_dps),(":class_name", class)]).unwrap();
          }
          None => {}
        };
      }
    }
  }

  fn parse_sql_result_dps(rows: Result<Rows, Error>) -> HashMap<String, HashMap<i64, i64>>{
    let mut rows = rows.unwrap();
    let mut data = HashMap::new();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let count: i64 = row.get(0);
      let class: String = row.get(1);
      let dps: i64 = row.get(2);
      let stat = data.entry(class).or_insert(HashMap::new());
      stat.insert(dps, count);
    }
    data
  }

  pub fn export_class(conn: &Connection, region: &str, date_start: i64, date_end: i64, area: i32, boss: i32)-> Result<HashMap<String, i64>,Error>{

    let table_name = get_table_name(area, boss, region, date_start, date_end);
    let sql = format!("SELECT count(1), class_name from {} group by class_name", table_name);
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_named(&[]);
    let mut rows = rows.unwrap();
    let mut result = HashMap::new();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let count: i64 = row.get(0);
      let name: String = row.get(1);
      result.insert(name, count);
    }
    Ok(result)
  }

  pub fn export_dps(conn: &Connection, region: &str, date_start: i64, date_end: i64, dps_max: i64, area:i32, boss: i32, dps_steps: i64)-> Result<HashMap<String, String>, Error>{

    let table_name = get_table_name(area, boss, region, date_start, date_end);
    let sql = format!("SELECT count(1), class_name, dps_stepped from {} group by class_name, dps_stepped", table_name);
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_named(&[]);
    let mut final_result = HashMap::new();
    let results = parse_sql_result_dps(rows);
    for result in results{
      let class = result.0;
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
      final_result.insert(class, data_str);
    }
    Ok(final_result)
  }


  pub fn export_median_dps(conn: &Connection, region: &str, date_start: i64, date_end: i64, area: i32, boss: i32)->Result<String, CountError>{
    let mut result = String::new();
    for c in CLASS{
      let table_name = get_table_name(area, boss, region, date_start, date_end);
      let sql = format!("SELECT AVG(dps) FROM (
      SELECT dps FROM {table} WHERE class_name = :class ORDER BY dps LIMIT 2 - (
      SELECT COUNT(*) FROM {table} where class_name = :class) % 2 OFFSET (
      SELECT (COUNT(*) - 1) / 2 FROM {table} where class_name = :class ))", table = table_name);
      let res = conn.query_row_named(&sql, &[(":class",c)], |row| {
        let median: Option<f64> = row.get(0);
        if median.is_some(){
          let line = format!("{}:{}\n", c, median.unwrap());
          result.push_str(&line);
        }
      });
      match res{
        Ok(o) => o,
        Err(e) => {println!("{:?}",e); continue;},
      };
    }
    if result.is_empty(){
      return Err(CountError::NoData);
    }
    Ok(result)
  }

  pub fn export_90_percentile_dps(conn: &Connection, region: &str, date_start: i64, date_end: i64, area: i32, boss: i32)->Result<String, CountError> {
    let mut result = String::new();
    for c in CLASS{
      let table_name = get_table_name(area, boss, region, date_start, date_end);
      let sql = format!("SELECT dps FROM {table} WHERE class_name = :class ORDER BY dps ASC LIMIT 1 OFFSET (SELECT COUNT(*)  FROM {table} WHERE class_name = :class) * 9 / 10 - 1", table = table_name);
      let res = conn.query_row_named(&sql, &[(":class",c)], |row| {
        let percentile_90: Option<f64> = row.get(0);
        if percentile_90.is_some(){
          let line = format!("{}:{}\n", c, percentile_90.unwrap());
          result.push_str(&line);
        }
      });
      match res{
        Ok(o) => o,
        Err(e) => {println!("{:?}", e); continue;},
      };
    }
    if result.is_empty() {
      return Err(CountError::NoData);
    }
    Ok(result)
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
    let result = serde_json::from_str(&stdout);
    let result = match result{
      Ok(o)=> o,
      Err(e)=> {println!("error file {}", filename); panic!("{:?}",e)},
    };
    result
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
