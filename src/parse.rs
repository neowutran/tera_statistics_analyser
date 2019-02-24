extern crate serde;
extern crate serde_json;
extern crate xz2;
use self::serde::{de, Deserializer};
use self::xz2::read;
use std::{fmt, fs::File, io::prelude::*, marker::PhantomData};
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
        let mut result: Vec<StatsLog> = serde_json::from_str(&String::from_utf8(decompressed).map_err(|_| format!("UTF8 invalid {}", filename))?)
            .map_err(|e| format!("Unable to parse {}: {}", filename, e))?;
        result.retain(| ref one_fight| !contain_forbidden_buff(one_fight) && !contain_forbidden_server(one_fight));
        //result.retain(| ref one_fight| !contain_shit(one_fight, filename));
        Ok(result)
    }
}

fn contain_forbidden_buff(stat: &&StatsLog) -> bool{
    let illegal_buff = vec!["25", "26", "27", "28", "37", "31", "36", "33"];
    for member in &stat.content.members{
        for buff in &member.buff_uptime{
            if illegal_buff.contains(&&*(buff.key)) {
                //println!("Illegal buff found: {}", buff.key);
                return true;
            }
            if buff.key == "8888889" && buff.value.parse::<i16>().unwrap() > 50{
                //println!("Slaying found");
                return true;
            }
        }

    }
    //println!("Yukikoo");
    false
}

fn contain_forbidden_server(stat: &&StatsLog) -> bool{
    let directory_vec: Vec<&str> = stat.directory.split(".").collect();
    let region = directory_vec[0];
    if region == "EU" {
        for member in &stat.content.members{
            if member.player_server != "Killian" &&
                    member.player_server != "Seren" &&
                    member.player_server != "Mystel" &&
                    member.player_server != "Yurian"{
                        //println!("rejected server: {}", member.player_server);
                        return true;
                    }
        }
        return false;

    }

    false

}

fn contain_shit(stat: &&StatsLog, filename: &str)->bool{
    if stat.content.area_id != 444{
        return false;
    }
    for member in &stat.content.members{
        if member.player_class == "Ninja" && member.player_dps.parse::<u64>().unwrap() > 9_000_000{
            println!("filename: {}", filename);
            println!("party size: {}", &stat.content.members.len());
            println!("trouve {}: {:?}", member.player_dps, member.buff_uptime);
            return false;
       }
    }
    return false;

}

// Full json structure
#[derive(Deserialize)]
pub struct StatsLog {
    pub content: Encounter,
    pub directory: String,
    //name: String,
}


#[derive(Deserialize,Debug)]
pub struct BuffUptime{
    #[serde(rename="Key")]
    pub key: String,
    #[serde(rename="Value")]
    pub value: String,
}

fn u32_from_str_or_int<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrInt(PhantomData<u32>);
    impl<'de> de::Visitor<'de> for StringOrInt {
        type Value = u32;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or int")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.parse::<u32>().unwrap())
        }
        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value as u32)
        }
        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value as u32)
        }
    }

    deserializer.deserialize_any(StringOrInt(PhantomData))
}
fn u64_from_str_or_int<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrInt(PhantomData<u64>);
    impl<'de> de::Visitor<'de> for StringOrInt {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or int")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.parse::<u64>().unwrap())
        }
        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value as u64)
        }
        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value as u64)
        }
    }

    deserializer.deserialize_any(StringOrInt(PhantomData))
}
#[derive(Deserialize)]
pub struct Encounter {
    #[serde(rename = "areaId", deserialize_with = "u32_from_str_or_int")]
    pub area_id: u32,
    #[serde(rename = "bossId", deserialize_with = "u32_from_str_or_int")]
    pub boss_id: u32,
    //#[serde(rename="debuffDetail")]
    //debuff_detail: Vec<Vec<Value>>,
    //#[serde(rename="debuffUptime")]
    //debuff_uptime: Vec<Value>,
    //#[serde(rename="encounterUnixEpoch")]
    //encounter_unix_epoch: i64,
    #[serde(rename = "fightDuration", deserialize_with = "u64_from_str_or_int")]
    pub fight_duration: u64,
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
    #[serde(rename="buffUptime")]
    buff_uptime: Vec<BuffUptime>,
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
