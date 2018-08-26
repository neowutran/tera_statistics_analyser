use bidir_map::BidirMap;
use parse::StatsLog;
use std::collections::HashMap;

#[derive(PartialEq, Hash, Eq, Clone)]
pub enum Class {
    Archer,
    Berserker,
    Brawler,
    Gunner,
    Lancer,
    Mystic,
    Ninja,
    Priest,
    Reaper,
    Slayer,
    Sorcerer,
    Valkyrie,
    Warrior,
}

pub struct DataDetails {
    pub dps: Vec<u32>,
    pub stepped_dps: HashMap<u32, u32>,
}

impl DataDetails {
    fn new() -> DataDetails {
        DataDetails {
            dps: Vec::new(),
            stepped_dps: HashMap::new(),
        }
    }

    fn add(&mut self, new_dps: u32, new_stepped: u32) {
        self.dps.push(new_dps);
        *(self.stepped_dps.entry(new_stepped).or_insert(0)) += 1;
    }
}

pub struct DungeonData {
    pub members: HashMap<Class, DataDetails>,
    pub healers_number: HashMap<u8, u32>,
    pub clear_time: Vec<u64>,
}

impl DungeonData {
    fn new() -> DungeonData {
        DungeonData {
            members: HashMap::new(),
            healers_number: HashMap::new(),
            clear_time: Vec::new(),
        }
    }
}

pub type Data = HashMap<String, DungeonData>;
pub type GlobalData = HashMap<Fight, Data>;

#[derive(Eq, PartialEq, Hash)]
pub struct Fight {
    pub area_id: u32,
    pub boss_id: u32,
}

impl Fight {
    fn new(area_id: u32, boss_id: u32) -> Fight {
        Fight {
            area_id: area_id,
            boss_id: boss_id,
        }
    }
    pub fn to_str(&self) -> String {
        format!("{}-{}", self.area_id, self.boss_id)
    }
}

pub fn get_key(region: &str, patch_name: &str) -> String {
    format!("{}-{}", region, patch_name)
}

fn get_patch_name(
    region_map: &HashMap<&str, HashMap<&str, (u64, u64)>>,
    region: &str,
    timestamp: u64,
) -> Option<String> {
    let region_data = match region_map.get(region) {
        Some(t) => t,
        None => return None,
    };
    for (patch_name, patch_date) in region_data {
        if patch_date.0 < timestamp && patch_date.1 > timestamp {
            return Some(patch_name.to_string());
        }
    }
    None
}

pub fn store(
    contents: Vec<StatsLog>,
    dps_steps: u32,
    data: &mut GlobalData,
    class_map: &BidirMap<&str, Class>,
    region_map: &HashMap<&str, HashMap<&str, (u64, u64)>>,
) {
    for content in contents {
        let directory_vec: Vec<&str> = content.directory.split(".").collect();
        let region = directory_vec[0];
        let timestamp = content.content.timestamp;
        let patch_name = match get_patch_name(region_map, region, timestamp) {
            Some(t) => t,
            None => continue,
        };
        let fight = Fight::new(content.content.area_id, content.content.boss_id);
        let key = get_key(region, &patch_name);
        let dungeon_data = data.entry(fight)
            .or_insert(Data::new())
            .entry(key)
            .or_insert(DungeonData::new());
        dungeon_data.clear_time.push(content.content.fight_duration);
        let mut healers_number: u8 = 0;
        for member in content.content.members {
            let class = match class_map.get_by_first(&&*(member.player_class)) {
                Some(c) => c,
                None => continue,
            };
            let mut dps: u32 = 0;
            match member.player_dps.parse() {
                Ok(value) => dps = value,
                Err(_) => {}
            };
            let stepped_dps = ((dps / dps_steps) as u32) * dps_steps;
            if class == &Class::Mystic || class == &Class::Priest {
                healers_number += 1;
            }
            dungeon_data
                .members
                .entry(class.clone())
                .or_insert(DataDetails::new())
                .add(dps, stepped_dps);
        }
        *(dungeon_data
            .healers_number
            .entry(healers_number)
            .or_insert(0)) += 1;
    }
}

pub struct ExportResult {
    pub class: HashMap<Class, ExportClass>,
    pub healers_number: HashMap<u8, u32>,
    pub clear_time_median: u64,
    pub clear_time_percentile_90: u64,
}

pub struct ExportClass {
    pub count: usize,
    pub dps_median: u32,
    pub dps_percentile_90: u32,
    pub stepped_dps: HashMap<u32, u32>,
}

impl ExportResult {
    fn new() -> ExportResult {
        ExportResult {
            class: HashMap::new(),
            healers_number: HashMap::new(),
            clear_time_median: 0,
            clear_time_percentile_90: 0,
        }
    }
}

pub fn export(mut raw_data: DungeonData, class_map: &BidirMap<&str, Class>) -> ExportResult {
    let mut result = ExportResult::new();
    result.healers_number = raw_data.healers_number;
    raw_data.clear_time.sort();
    result.clear_time_median = raw_data.clear_time[(raw_data.clear_time.len() / 2) as usize];
    result.clear_time_percentile_90 =
        raw_data.clear_time[((raw_data.clear_time.len() as f32 * 0.1) as usize)];
    for class in class_map.iter_second_first() {
        let mut data = match raw_data.members.remove(class) {
            Some(t) => t,
            None => continue,
        };
        data.dps.sort();
        result.class.insert(
            class.clone(),
            ExportClass {
                count: data.dps.len(),
                dps_median: data.dps[(data.dps.len() / 2) as usize],
                dps_percentile_90: data.dps[(data.dps.len() as f32 * 0.9) as usize],
                stepped_dps: data.stepped_dps,
            },
        );
    }
    result
}
