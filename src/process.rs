use time_slice::TimeSlice;
use parse::StatsLog;
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

pub fn store(conn: &Connection, contents: &Vec<StatsLog>, time_slice: &TimeSlice, dps_steps: i64, area_boss: &mut HashSet<(i32, i32)>) {
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
