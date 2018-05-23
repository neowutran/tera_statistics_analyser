extern crate serde_json;
extern crate xz2;
use self::xz2::read;
use std::fs::File;
use std::io::prelude::*;
impl StatsLog {
    pub fn new(filename: &String) -> Result<Vec<StatsLog>, String> {
        let mut decompressed = Vec::new();
        {
            let mut compressed = Vec::new();
            File::open(filename)
                .unwrap()
                .read_to_end(&mut compressed)
                .map_err(|_| format!("Unable to open {}", filename))?;
            read::XzDecoder::new(&*compressed)
                .read_to_end(&mut decompressed)
                .map_err(|_| format!("Unable to decompress {}", filename))?;
        }
        let result = serde_json::from_str(&String::from_utf8(decompressed).map_err(|_| format!("UTF8 invalid {}", filename))?)
            .map_err(|_| format!("Unable to parse {}", filename))?;
        Ok(result)
    }
}

// Full json structure
#[derive(Deserialize)]
pub struct StatsLog {
    pub content: Encounter,
    pub directory: String,
    //name: String,
}

#[derive(Deserialize)]
pub struct Encounter {
    #[serde(rename = "areaId")]
    pub area_id: String,
    #[serde(rename = "bossId")]
    pub boss_id: String,
    //#[serde(rename="debuffDetail")]
    //debuff_detail: Vec<Vec<Value>>,
    //#[serde(rename="debuffUptime")]
    //debuff_uptime: Vec<Value>,
    //#[serde(rename="encounterUnixEpoch")]
    //encounter_unix_epoch: i64,
    #[serde(rename = "fightDuration")]
    pub fight_duration: String,
    pub timestamp: u64,
    pub members: Vec<Members>,
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
pub struct Members {
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
    #[serde(rename = "playerClass")]
    pub player_class: String,
    //#[serde(rename="playerDeathDuration")]
    //player_death_duration: String,
    //#[serde(rename="playerDeaths")]
    //player_deaths: String,
    #[serde(rename = "playerDps")]
    pub player_dps: String,
    //#[serde(rename="playerId")]
    //player_id:u32,
    //#[serde(rename = "playerName")]
    //pub player_name: String,
    #[serde(rename = "playerServer")]
    pub player_server: String,
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
pub struct SkillLog {
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
