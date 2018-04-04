use time_slice::TimeSlice;
use parse::StatsLog;
use std::{
  collections::{HashMap},
};
pub const CLASS: &'static [&'static str] = &["Archer","Berserker","Brawler","Gunner","Lancer","Mystic","Ninja","Priest","Reaper","Slayer","Sorcerer","Valkyrie","Warrior"];

pub struct DataDetails{
  pub dps: Vec<i64>,
  pub stepped_dps: HashMap<i64, i64>,
}

impl DataDetails{
  fn new() -> DataDetails{
    DataDetails{dps:Vec::new(), stepped_dps: HashMap::new()}
  }

  fn add(&mut self, new_dps: i64, new_stepped: i64){
    self.dps.push(new_dps);
    let old_value = *self.stepped_dps.entry(new_stepped).or_insert(0);
    self.stepped_dps.insert(new_stepped, old_value + 1);
  }
  fn size(&self) -> usize{
    self.dps.len()
  }
}

pub struct DungeonData{
  pub members : HashMap<String, DataDetails>,
  pub healers_number: HashMap<i8, i64>,
}

impl DungeonData{
  fn new() -> DungeonData{
    DungeonData{members:HashMap::new(), healers_number: HashMap::new()}
  }
}

pub type Data = HashMap<String, DungeonData>;
pub type GlobalData = HashMap<Fight, Data>;

#[derive(Eq, PartialEq, Hash)]
pub struct Fight{
  pub area_id: i32,
  pub boss_id: i32,
}

impl Fight{
  fn new(area_id: i32, boss_id: i32) -> Fight{
    Fight{area_id: area_id, boss_id: boss_id }
  }
  pub fn to_str(&self)-> String{
    format!("{}-{}", self.area_id, self.boss_id)
  }
}

pub fn get_key(region: &str, time: &(i64, i64)) -> String{
  format!("{}-{}-{}", region, time.0, time.1)
}

pub fn store(contents: &Vec<StatsLog>, time_slice: &TimeSlice, dps_steps: i64, mut data: GlobalData) -> GlobalData {
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
    let mut healers_number: i8 = 0;
    for member in &content.content.members{
      let class = &member.player_class;
      match CLASS.iter().find(|&&c| c == class){
        Some(_) => {
          let dps: i64 = member.player_dps.parse().unwrap();
          let stepped_dps = ((dps / dps_steps) as i64) * dps_steps;
          let dps_details = dungeon_data.members.entry(class.clone()).or_insert(DataDetails::new());
          dps_details.add(dps, stepped_dps);
          if class == "Mystic" || class == "Priest" {
            healers_number += 1;
          }
        }
        None => {}
      };
    }
    let mut old_value = dungeon_data.healers_number.entry(healers_number).or_insert(0);
    *old_value += 1;
  }
  data
}

pub struct ExportResult{
  pub class: HashMap<String, ExportClass>,
  pub healers_number: HashMap<i8, i64>,
}

pub struct ExportClass{
  pub count: usize,
  pub median: i64,
  pub percentile_90: i64,
  pub stepped_dps: HashMap<i64,i64>
}

impl ExportResult{
  fn new() -> ExportResult{
    ExportResult{class: HashMap::new(), healers_number:HashMap::new()}
  }
}

pub fn export(mut raw_data: DungeonData)-> ExportResult {
  let mut result = ExportResult::new();
  result.healers_number = raw_data.healers_number;
  for class in CLASS{
    let mut data = match raw_data.members.remove(*class){
      Some(t) => t,
      None => continue,
    };
    data.dps.sort();
    result.class.insert(String::from(*class),
    ExportClass{
      count: data.size(),
      median: data.dps[(data.size() / 2 ) as usize],
      percentile_90: data.dps[(data.size() as f64 * 0.9) as usize],
      stepped_dps: data.stepped_dps
    });
  }
  result
}
