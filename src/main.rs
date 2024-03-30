use std::{fs::File, io::{self, BufReader, Write}, sync::Arc, time::{Duration, Instant}};
use reqwest::Client;
use rand::prelude::SliceRandom;
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgencyInfo {
    pub onetrip: String,
    pub realtime_vehicle_positions: String,
    pub realtime_trip_updates: String,
    pub realtime_alerts: String,
    pub has_auth: bool,
    pub auth_type: String,
    pub auth_header: String,
    pub auth_password: String,
    pub fetch_interval: f32,
    pub multiauth: Option<Vec<String>>,
    pub last_update: u64,
}


#[derive(Debug)]
pub struct Agencyurls {
    pub vehicles: Option<String>,
    pub trips: Option<String>,
    pub alerts: Option<String>,
}

pub fn persist_gtfs_rt(
    data: &gtfs_rt::FeedMessage,
    onetrip: &str,
    category: &str,
) -> io::Result<()> {
    let now_millis = data.header.timestamp.unwrap_or(0);
    if now_millis == 0 {
        return Ok(());
    }
    let bytes: Vec<u8> = data.encode_to_vec();
    let file_path = format!("./gtfs-rt/{}-{}-{}", onetrip, category, now_millis);
    let mut file = File::create(file_path)?;
    file.write_all(&bytes)
}

pub fn parse_protobuf_message(
    bytes: &[u8],
) -> Result<gtfs_rt::FeedMessage, Box<dyn std::error::Error>> {
    let x = prost::Message::decode(bytes);

    if x.is_ok() {
        return Ok(x.unwrap());
    } else {
        return Err(Box::new(x.unwrap_err()));
    }
}

pub async fn fetchurl(url: &Option<String>, auth_header: &String, auth_type: &String, auth_password: &String, client: &reqwest::Client, timeoutforfetch: u64) -> Option<Vec<u8>> {
    let mut req = client.get(url.to_owned().unwrap());

    if auth_type == "header" {
        req = req.header(auth_header, auth_password);
    }

    let resp = req.timeout(Duration::from_millis(timeoutforfetch)).send().await;

    match resp {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.bytes().await {
                    Ok(bytes_pre) => {
                        let bytes = bytes_pre.to_vec();
                        Some(bytes)
                    }
                    _ => None,
                }
            } else {
                println!("{}:{:?}", &url.clone().unwrap(), resp.status());
                None
            }
        }
        Err(e) => {
            println!("error fetching url: {:?}", e);
            None
        }
    }
}

pub fn make_url(url: &String, auth_type: &String, auth_header: &String, auth_password: &String) -> Option<String> {
    if !url.is_empty() {
        let mut outputurl = url.clone();

        if !auth_password.is_empty() && auth_type == "query_param" {
            outputurl = outputurl.replace("PASSWORD", &auth_password);
        }

        return Some(outputurl);
    }
    return None;
}

async fn fetchagency(client: &Client, agency: AgencyInfo)  {
    let mut timestamp_trips = 0;
    let mut timestamp_vehicles = 0;
    let mut timestamp_alerts = 0;
    loop {
        let time = Instant::now();
        let fetch = Agencyurls {
            vehicles: make_url(
                &agency.realtime_vehicle_positions,
                &agency.auth_type,
                &agency.auth_header,
                &agency.auth_password,
            ),
            trips: make_url(
                &agency.realtime_trip_updates,
                &agency.auth_type,
                &agency.auth_header,
                &agency.auth_password,
            ),
            alerts: make_url(
                &agency.realtime_alerts,
                &agency.auth_type,
                &agency.auth_header,
                &agency.auth_password,
            ),
        };

        let passwordtouse = match &agency.multiauth {
            Some(multiauth) => {
                let mut rng = rand::thread_rng();
                let random_auth = multiauth.choose(&mut rng).unwrap();

                random_auth.to_string()
            }
            None => agency.auth_password.clone(),
        };

        let fetch_vehicles = {
            fetchurl(
                &fetch.vehicles,
                &agency.auth_header,
                &agency.auth_type,
                &passwordtouse,
                &client,
                15_000,
                //timeoutforfetch,
            )
        };
        
        let fetch_trips = {
            fetchurl(
                &fetch.trips,
                &agency.auth_header,
                &agency.auth_type,
                &passwordtouse,
                &client,
                15_000,
                //timeoutforfetch,
            )
        };
        
        let fetch_alerts = {
            fetchurl(
                &fetch.alerts,
                &agency.auth_header,
                &agency.auth_type,
                &passwordtouse,
                &client,
                15_000,
                //timeoutforfetch,
            )
        };
        
        let vehicles_result = fetch_vehicles.await;
        let trips_result = fetch_trips.await;
        let alerts_result = fetch_alerts.await;

        if vehicles_result.is_some() {
            let bytes = vehicles_result.as_ref().unwrap().to_vec();
            
            println!("{} vehicles bytes: {}", &agency.onetrip, bytes.len());

            let data = parse_protobuf_message(&bytes).unwrap();
            if data.header.timestamp.unwrap() > timestamp_vehicles && !data.entity.is_empty() {
                timestamp_vehicles = data.header.timestamp.unwrap();
                let _ = persist_gtfs_rt(&data, &agency.onetrip, "vehicles");
            }
        }

        if trips_result.is_some() {
            let bytes = trips_result.as_ref().unwrap().to_vec();

            println!("{} trips bytes: {}", &agency.onetrip, bytes.len());

            let data = parse_protobuf_message(&bytes).unwrap();
            if data.header.timestamp.unwrap() > timestamp_trips && !data.entity.is_empty() {
                timestamp_trips = data.header.timestamp.unwrap();
                let _ = persist_gtfs_rt(&data, &agency.onetrip,"trips");
            }
        }

        if alerts_result.is_some() {
            let bytes = alerts_result.as_ref().unwrap().to_vec();

            println!("{} alerts bytes: {}", &agency.onetrip, bytes.len());

            let data = parse_protobuf_message(&bytes).unwrap();
            if data.header.timestamp.unwrap() > timestamp_alerts && !data.entity.is_empty() {
                timestamp_alerts = data.header.timestamp.unwrap();
                let _ = persist_gtfs_rt(&data, &agency.onetrip,"alerts");
            }
        }
        let duration = time.elapsed().as_secs_f32();
        if duration < agency.fetch_interval {
            let sleep_duration: f32 = agency.fetch_interval - duration;
            println!("sleeping for {:?}", sleep_duration);
            std::thread::sleep(Duration::from_secs_f32(sleep_duration));
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args();
    let arguments = arguments::parse(arguments).unwrap();

    let filenametouse = match arguments.get::<String>("urls") {
        Some(filename) => filename,
        None => String::from("urls.csv"),
    };

    let timeoutforfetch = match arguments.get::<u64>("timeout") {
        Some(filename) => filename,
        None => 15_000,
    };

    let threadcount = match arguments.get::<usize>("threads") {
        Some(threadcount) => threadcount,
        None => 50,
    };
    let file = File::open(filenametouse).unwrap();
    let mut reader = csv::Reader::from_reader(BufReader::new(file));

    let mut agencies: Vec<AgencyInfo> = Vec::new();

    for record in reader.records() {
        match record {
            Ok(record) => {
                let agency = AgencyInfo {
                    onetrip: record[0].to_string(),
                    realtime_vehicle_positions: record[1].to_string(),
                    realtime_trip_updates: record[2].to_string(),
                    realtime_alerts: record[3].to_string(),
                    has_auth: record[4].parse().unwrap(),
                    auth_type: record[5].to_string(),
                    auth_header: record[6].to_string(),
                    auth_password: record[7].to_string(),
                    fetch_interval: record[8].parse().unwrap(),
                    multiauth: {
                        if !record[9].to_string().is_empty() {
                            let mut outputvec: Vec<String> = Vec::new();
                            for s in record[9].to_string().clone().split(",") {
                                outputvec.push(s.to_string());
                            }
                            Some(outputvec)
                        } else {
                            None
                        }
                    },
                    last_update: 0,
                };
                agencies.push(agency);
            }
            Err(e) => {
                println!("error reading csv record");
                println!("{:?}", e);
            }
        }
    }

    let client = Arc::new(reqwest::ClientBuilder::new()
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .build()
        .unwrap());

    let mut handles = Vec::new();
    for agency in agencies {
        let client = Arc::clone(&client);
        let handle = tokio::spawn(async move {
            fetchagency(&client, agency).await;
        });
        handles.push(handle);
    };

    for handle in handles {
        let _ = handle.await.unwrap();
    }
    Ok(())
}
