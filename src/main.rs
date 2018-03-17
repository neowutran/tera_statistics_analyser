#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate lzma;
extern crate serde_json;
extern crate rusqlite;
use serde_json::{Error};
use docopt::Docopt;
use std::fs;
use std::str;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::io::prelude::*;
use rusqlite::Connection;
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

#[derive(Debug, Deserialize)]
struct Args {
  flag_source: String,
  flag_target: String,
}

fn main() {
  let args: Args = Docopt::new(USAGE)
    .and_then(|d| d.deserialize())
    .unwrap_or_else(|e| e.exit());
  println!("Source directory is {}", args.flag_source);
  let mut db = database_initialization();
  let paths = fs::read_dir(args.flag_source).unwrap();
  for path in paths {
    let name = path.unwrap().path();
    let name = name.to_str().unwrap();
    if name.ends_with(".xz"){
      println!("{}", name);
      let contents = StatsLog::new(&name);
      db = class_count::process(contents, db);
    }
  }

  class_count::export(db);
}

fn write_file(name: String, content: String){
  let path = Path::new(&name);
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

fn database_initialization() -> Connection{
  let mut conn = Connection::open_in_memory().unwrap();
  conn = class_count::initialize(conn);
  return conn;
}

mod class_count{
  use StatsLog;
  use rusqlite::Connection;
  pub fn initialize(db: Connection) -> Connection{
    db.execute("CREATE TABLE player_class (
                  id              INTEGER PRIMARY KEY,
                  name            TEXT NOT NULL,
                  region          TEXT NOT NULL,
                  dungeon_id      INTEGER NOT NULL,
                  boss_id         INTEGER NOT NULL,
                  time            INTEGER NOT NULL
                  )", &[]).unwrap();
    return db
  }

  pub fn process(contents: Vec<StatsLog>, db: Connection) -> Connection {
    for content in contents{
      let directory_split = content.directory.split(".");
      let directory_vec: Vec<&str> = directory_split.collect();
      let region = String::from(directory_vec[0]);
      let timestamp = content.content.timestamp;
      let dungeon:i32 = content.content.area_id.parse().unwrap();
      let boss:i32 = content.content.boss_id.parse().unwrap();
      for member in content.content.members{
        let class = member.player_class;
        db.execute_named("INSERT INTO player_class (name, region, dungeon_id, boss_id, time)
                  VALUES (:name, :region, :dungeon_id, :boss_id, :time)", &[(":name", &class), (":region",&region), (":dungeon_id",&dungeon), (":boss_id",&boss), (":time",&timestamp)]).unwrap();
      }
    }
    return db;
  }
  pub fn export(db: Connection) -> Connection{
    {
      let mut stmt = db.prepare("SELECT count(1), name, region from player_class group by name, region").unwrap();
      let mut rows = stmt.query(&[]).unwrap();
      let mut global = String::new();
      while let Some(result_row) = rows.next() {
        let row = result_row.unwrap();
        let count: i64 = row.get(0);
        let name: String = row.get(1);
        let region: String = row.get(2);
        println!("{} {}: {}", region, name, count);
      }

    }
    return db;
  }
}



impl StatsLog{
  pub fn new(filename: &str) -> Vec<StatsLog>{
    println!("read {}", &filename);
    // Rust-lzma crash on magic byte detection for XZ. So back to system version until I found why
    let mut command = String::from("unxz --stdout ");
    command.push_str(filename);
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
  #[serde(rename="fightDuration")]
  fight_duration: String,
  timestamp: i64,
  members: Vec<Members>,
  //#[serde(rename="meterName")]
  //meter_name: String,
  //#[serde(rename="meterVersion")]
  //meter_version: String,
  #[serde(rename="partyDps")]
  party_dps: String,
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
  #[serde(rename="playerServer")]
  player_server: String,
  //#[serde(rename="playerTotalDamage")]
  //player_total_damage: String,
  #[serde(rename="playerTotalDamagePercentage")]
  player_total_damage_percentage: String,
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

