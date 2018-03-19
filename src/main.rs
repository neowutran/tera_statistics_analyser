#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate serde_json;
extern crate rusqlite;
extern crate glob;
extern crate r2d2;
extern crate time;
extern crate r2d2_sqlite;
extern crate threadpool;
extern crate chrono;
use docopt::Docopt;
use std::sync::mpsc::{Sender, Receiver};
use std::str;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::io::prelude::*;
use r2d2::Pool;
use r2d2::PooledConnection;
use glob::glob;
use std::sync::mpsc;
use r2d2_sqlite::SqliteConnectionManager;
use threadpool::ThreadPool;
use threadpool::Builder;
use std::fs;
use time::Duration;
use chrono::prelude::*;
const USAGE: &'static str = "
Tera Statistics Analyser.

Usage:
  tera_statistics_analyser [--source <source>] [--target <target>]
  tera_statistics_analyser (-h | --help)
  tera_statistics_analyser --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --source <source>   Source data directory [default: ./].
  --target <target>   Target data directory [default: ./].
";

const REGIONS: &'static [&'static str] = &["EU", "NA", "KR", "JP", "RU", "TW", "KR-PTS", "THA"];
#[derive(Debug, Deserialize)]
struct Args {
  flag_source: String,
  flag_target: String,
}
/**
 * - Find recursivly all .xz files from the source folder
 * - Create a create of each of those file. The thread decompressed them and parse them as Json
 * - For each of those json, create a new processing thread. Those threads store statistics inside sqlite in memory database.
 * - Export statistics to files
 *
 * TODO a little drawing on how it work
 **/
fn main() {
  let args: Args = Docopt::new(USAGE)
    .and_then(|d| d.deserialize())
    .unwrap_or_else(|e| e.exit());

  let database_pool = database_initialization();
  let database_pool_clone = database_pool.clone();
  initialize(&database_pool_clone);

  let (tx, rx): (Sender<Vec<StatsLog>>, Receiver<Vec<StatsLog>>) = mpsc::channel();
  let search = format!("{}/**/*.xz", args.flag_source);
  let thread_pool_decompress: ThreadPool = Builder::new().build();
  for entry in glob(&search).expect("Failed to read glob pattern") {
    let os_string = entry.unwrap().into_os_string();
    let string = os_string.into_string().unwrap();
    let thread_tx = tx.clone();
    // read the compressed file and parse the json
    thread_pool_decompress.execute(move || {
      let contents = StatsLog::new(&string);
      thread_tx.send(contents).unwrap();
      println!("Parsing finished for {}", string);
    });
  }

  drop(tx);
  let thread_pool_process: ThreadPool = Builder::new().build();
  for received in rx{
    let db_pool_clone = database_pool.clone();
    // Compute the statistics based on the parsed json
    thread_pool_process.execute(move || {
      process(&db_pool_clone, &received);
      println!("Processing ended");
    });
  }

  thread_pool_decompress.join();
  thread_pool_process.join();
  let db_pool_clone = database_pool.clone();
  export(&db_pool_clone, args.flag_target);
}

fn initialize(database_pool: &Pool<SqliteConnectionManager>){
  let conn = database_pool.get().unwrap();
  class_count::initialize(conn);
  let conn = database_pool.get().unwrap();
  dps_count::initialize(conn);
}

fn process(database_pool: &Pool<SqliteConnectionManager>, contents: &Vec<StatsLog>){
  let conn = database_pool.get().unwrap();
  class_count::process(conn, contents);
  let conn = database_pool.get().unwrap();
  dps_count::process(conn, contents);
}

fn export(database_pool: &Pool<SqliteConnectionManager>, target: String){
  let thread_pool: ThreadPool = Builder::new().build();
  let mut beginning = Utc.datetime_from_str("2018-03-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
  let current:i64 = Utc::now().timestamp();
  while beginning.timestamp() < current{
    let end = beginning + Duration::days(30);
    let database_pool_clone = database_pool.clone();
    let target_copy = target.clone();
    thread_pool.execute(move || {
      for region in REGIONS{
        let conn = database_pool_clone.get().unwrap();
        export_class(conn, &target_copy, region, beginning, end);
        let conn = database_pool_clone.get().unwrap();
        export_dps(conn, &target_copy, region, beginning, end);
      }
    });
    beginning = end;
  }
  thread_pool.join();
}

fn export_dps(conn: PooledConnection<SqliteConnectionManager>, target: &String, region: &str, date_start: DateTime<Utc>, date_end: DateTime<Utc>){
  let results = dps_count::export(conn, region, date_start, date_end );
  for result in results{
    let key = result.0;
    let boss_id = key.0;
    let dungeon_id = key.1;
    let class = key.2;
    let data = result.1;
    let boss_area = format!("{}-{}", dungeon_id, boss_id);
    let filename = format!("{}/{}/{}/{}/{}-{}.txt", target, boss_area, class, region, date_start.year(), date_start.month());
    write_file(filename, &data);
  }
}
fn export_class(conn: PooledConnection<SqliteConnectionManager>, target: &String, region: &str, date_start: DateTime<Utc>, date_end: DateTime<Utc>){
  let result = class_count::export(conn, region, date_start, date_end );
  let filename = format!("{}/{}/{}-{}.txt", target, region, date_start.year(), date_start.month());
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
    Ok(_) => println!("successfully wrote to {}", display),
  }
}

fn database_initialization() -> Pool<SqliteConnectionManager>{
  let manager = SqliteConnectionManager::memory();
  let pool = r2d2::Pool::builder()
    .max_size(1)
    .build(manager)
    .unwrap();
  return pool;
}



mod dps_count{
  use StatsLog;
  use r2d2_sqlite::SqliteConnectionManager;
  use r2d2::PooledConnection;
  use chrono::prelude::*;
  use rusqlite::Rows;
  use rusqlite::Error;
  use std::collections::HashMap;
  pub fn initialize(conn: PooledConnection<SqliteConnectionManager>){
    conn.execute("CREATE TABLE dps (
                  id              INTEGER PRIMARY KEY,
                  dps             INTEGER NOT NULL,
                  class           TEXT NOT NULL,
                  region          TEXT NOT NULL,
                  dungeon_id      INTEGER NOT NULL,
                  boss_id         INTEGER NOT NULL,
                  time            INTEGER NOT NULL
                  )", &[]).unwrap();
  }

  pub fn process(conn: PooledConnection<SqliteConnectionManager>, contents: &Vec<StatsLog>) {
    for content in contents{
      let directory_split = content.directory.split(".");
      let directory_vec: Vec<&str> = directory_split.collect();
      let region = String::from(directory_vec[0]);
      let timestamp = content.content.timestamp;
      let dungeon:i32 = content.content.area_id.parse().unwrap();
      let boss:i32 = content.content.boss_id.parse().unwrap();
      for member in &content.content.members{
        let class = &member.player_class;
        let dps: i64 = member.player_dps.parse().unwrap();
        let dps_cat = (dps / 10000) as i32;
        conn.execute_named("INSERT INTO dps (dps, class, region, dungeon_id, boss_id, time)
                  VALUES (:dps, :class, :region, :dungeon_id, :boss_id, :time)", &[(":dps", &dps_cat),(":class", class), (":region",&region), (":dungeon_id",&dungeon), (":boss_id",&boss), (":time",&timestamp)]).unwrap();
      }
    }
  }

  fn parse_sql_result(rows: Result<Rows, Error>) -> HashMap<(i32, i32, String), String>{
    let mut rows = rows.unwrap();
    let mut data = HashMap::new();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let count: i64 = row.get(0);
      let class: String = row.get(1);
      let dps: i32 = row.get(2);
      let boss_id: i32 = row.get(3);
      let area_id: i32 = row.get(4);
      let mut line = format!("{}:{}\n", dps, count);
      let key = (boss_id, area_id, class);
      if data.contains_key(&key){
        let existing_line = data.get(&key).unwrap();
        line = format!("{}{}", existing_line, line);
      }
      data.insert(key, line);
    }
    return data;

  }

  pub fn export(conn: PooledConnection<SqliteConnectionManager>, region: &str, date_start: DateTime<Utc>, date_end: DateTime<Utc>)-> HashMap<(i32, i32, String), String>{
    let mut stmt = conn.prepare("SELECT count(1), class, dps, boss_id, dungeon_id from dps where region = :region and time >= :date_start and time <= :date_end group by class, dps, boss_id, dungeon_id").unwrap();
    let rows = stmt.query_named(&[(":region", &region), (":date_start", &date_start.timestamp()), (":date_end", &date_end.timestamp())]);
    return parse_sql_result(rows);
  }
}

mod class_count{
  use StatsLog;
  use r2d2_sqlite::SqliteConnectionManager;
  use r2d2::PooledConnection;
  use chrono::prelude::*;
  use rusqlite::Rows;
  use rusqlite::Error;
  pub fn initialize(conn: PooledConnection<SqliteConnectionManager>){
    conn.execute("CREATE TABLE player_class (
                  id              INTEGER PRIMARY KEY,
                  class           TEXT NOT NULL,
                  region          TEXT NOT NULL,
                  dungeon_id      INTEGER NOT NULL,
                  boss_id         INTEGER NOT NULL,
                  time            INTEGER NOT NULL
                  )", &[]).unwrap();
  }

  pub fn process(conn: PooledConnection<SqliteConnectionManager>, contents: &Vec<StatsLog>) {
    for content in contents{
      let directory_split = content.directory.split(".");
      let directory_vec: Vec<&str> = directory_split.collect();
      let region = String::from(directory_vec[0]);
      let timestamp = content.content.timestamp;
      let dungeon:i32 = content.content.area_id.parse().unwrap();
      let boss:i32 = content.content.boss_id.parse().unwrap();
      for member in &content.content.members{
        let class = &member.player_class;
        conn.execute_named("INSERT INTO player_class (class, region, dungeon_id, boss_id, time)
                  VALUES (:class, :region, :dungeon_id, :boss_id, :time)", &[(":class", class), (":region",&region), (":dungeon_id",&dungeon), (":boss_id",&boss), (":time",&timestamp)]).unwrap();
      }
    }
  }

  fn parse_sql_result(rows: Result<Rows, Error>) -> String{
    let mut rows = rows.unwrap();
    let mut result = String::new();
    while let Some(result_row) = rows.next() {
      let row = result_row.unwrap();
      let count: i64 = row.get(0);
      let name: String = row.get(1);
      let line = format!("{}:{}\n", name, count);
      result.push_str(&line);
    }
    return result;

  }

  pub fn export(conn: PooledConnection<SqliteConnectionManager>, region: &str, date_start: DateTime<Utc>, date_end: DateTime<Utc>)-> String{
    let mut stmt = conn.prepare("SELECT count(1), class from player_class where region = :region and time >= :date_start and time <= :date_end group by class").unwrap();
    let rows = stmt.query_named(&[(":region", &region), (":date_start", &date_start.timestamp()), (":date_end", &date_end.timestamp())]);
    return parse_sql_result(rows);
  }
}

impl StatsLog{
  pub fn new(filename: &String) -> Vec<StatsLog>{
    println!("read {}", filename);
    // Rust-lzma crash on magic byte detection for XZ. So back to system version until I found why.
    // To improve and find why rust-lzma doesn't work.
    let mut command = String::from("unxz --stdout ");
    command.push_str(&filename);
    println!("command: {}", command);
    let output = Command::new("sh")
      .arg("-c")
      .arg(command)
      .output()
      .expect("failed to execute process");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: Vec<StatsLog> = serde_json::from_str(&stdout).unwrap();
    return json;
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
