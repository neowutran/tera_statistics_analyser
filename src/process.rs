use time_slice::TimeSlice;
use parse::StatsLog;
use std::{
  collections::{HashMap},
};
const CLASS: &'static [&'static str] = &["Archer","Berserker","Brawler","Gunner","Lancer","Mystic","Ninja","Priest","Reaper","Slayer","Sorcerer","Valkyrie","Warrior"];

pub struct DataDetails{
  pub dps: Vec<u32>,
  pub stepped_dps: HashMap<u32, u32>,
}

impl DataDetails{
  fn new() -> DataDetails{
    DataDetails{dps:Vec::new(), stepped_dps: HashMap::new()}
  }

  fn add(&mut self, new_dps: u32, new_stepped: u32){
    self.dps.push(new_dps);
    *(self.stepped_dps.entry(new_stepped).or_insert(0)) +=1;
  }
}

pub struct DungeonData{
  pub members : HashMap<String, DataDetails>,
  pub healers_number: HashMap<u8, u32>,
  pub clear_time: Vec<u32>,
}

impl DungeonData{
  fn new() -> DungeonData{
    DungeonData{members:HashMap::new(), healers_number: HashMap::new(), clear_time: Vec::new()}
  }
}

pub type Data = HashMap<String, DungeonData>;
pub type GlobalData = HashMap<Fight, Data>;

#[derive(Eq, PartialEq, Hash)]
pub struct Fight{
  pub area_id: u32,
  pub boss_id: u32,
}

impl Fight{
  fn new(area_id: u32, boss_id: u32) -> Fight{
    Fight{area_id: area_id, boss_id: boss_id }
  }
  pub fn to_str(&self)-> String{
    format!("{}-{}", self.area_id, self.boss_id)
  }
}

pub fn get_key(region: &str, time: &(u64, u64)) -> String{
  format!("{}-{}-{}", region, time.0, time.1)
}

pub fn store(contents: Vec<StatsLog>, time_slice: &TimeSlice, dps_steps: u32, mut data: GlobalData) -> GlobalData {
  for content in contents{
    let timestamp = content.content.timestamp;
    let time = match time_slice.get_time_slice(timestamp){
      Some(t) => t,
      None => continue,
    };
    let directory_vec: Vec<&str> = content.directory.split(".").collect();
    let fight = Fight::new(content.content.area_id.parse().unwrap(), content.content.boss_id.parse().unwrap());
    let key = get_key(directory_vec[0], &time);
    let dungeon_data = data.entry(fight).or_insert(Data::new()).entry(key).or_insert(DungeonData::new());
    dungeon_data.clear_time.push(content.content.fight_duration.parse::<u32>().unwrap());
    let mut healers_number: u8 = 0;
    for member in content.content.members{
      let class = member.player_class;
      let dps: u32 = member.player_dps.parse().unwrap();
      let stepped_dps = ((dps / dps_steps) as u32) * dps_steps;
      if class == "Mystic" || class == "Priest" {
        healers_number += 1;
      }
      dungeon_data.members.entry(class).or_insert(DataDetails::new()).add(dps, stepped_dps);
    }
    *(dungeon_data.healers_number.entry(healers_number).or_insert(0)) += 1;
  }
  data
}

pub struct ExportResult{
  pub class: HashMap<String, ExportClass>,
  pub healers_number: HashMap<u8, u32>,
  pub clear_time_median: u32,
  pub clear_time_percentile_90: u32,
}

pub struct ExportClass{
  pub count: usize,
  pub dps_median: u32,
  pub dps_percentile_90: u32,
  pub stepped_dps: HashMap<u32,u32>
}

impl ExportResult{
  fn new() -> ExportResult{
    ExportResult{class: HashMap::new(), healers_number:HashMap::new(), clear_time_median: 0, clear_time_percentile_90:0}
  }
}

pub fn export(mut raw_data: DungeonData)-> ExportResult {
  let mut result = ExportResult::new();
  result.healers_number = raw_data.healers_number;
  raw_data.clear_time.sort();
  result.clear_time_median = raw_data.clear_time[(raw_data.clear_time.len() / 2) as usize];
  result.clear_time_percentile_90 = raw_data.clear_time[((raw_data.clear_time.len() as f32 * 0.1) as usize)];
  for class in CLASS{
    let mut data = match raw_data.members.remove(*class){
      Some(t) => t,
      None => continue,
    };
    data.dps.sort();
    result.class.insert(String::from(*class),
    ExportClass{
      count: data.dps.len(),
      dps_median: data.dps[(data.dps.len() / 2 ) as usize],
      dps_percentile_90: data.dps[(data.dps.len() as f32 * 0.9) as usize],
      stepped_dps: data.stepped_dps
    });
  }
  result
}
