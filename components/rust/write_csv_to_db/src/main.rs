use std::error::Error;
use std::fs::File;
// use std::collections::HashMap;
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, DateTime, Utc};
use bson::{Bson, DateTime as BsonDateTime};
use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client, Collection,
};
 
#[derive(Debug, Deserialize, Serialize)]
pub struct CsvRecord {
    #[serde(rename = "Date")]
    pub date: String,  
    #[serde(rename = "Time")]
    pub time: String,
    #[serde(rename = "Open")]
    pub open: f64,
    #[serde(rename = "High")]
    pub high: f64,
    #[serde(rename = "Low")]
    pub low: f64,
    #[serde(rename = "Close")]
    pub close: f64,
    #[serde(rename = "Volume")]
    pub volume: i32,
    #[serde(rename = "Ticker")]
    pub ticker: String,
}
 
#[derive(Debug)]
pub struct CsvProcessor {
    pub file_path: String,
}
 
impl CsvProcessor {
    pub fn new(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
        }
    }
 
    pub fn read_csv(&self) -> Result<Vec<CsvRecord>, Box<dyn Error>> {
        let mut tick_records = Vec::new();
        let file = File::open(&self.file_path)?;
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
 
        for result in rdr.deserialize::<CsvRecord>() {
            let record = result?;
            tick_records.push(record);
        }
        Ok(tick_records)
    }
 
    pub fn to_records(&self, data: Vec<CsvRecord>) -> Result<Vec<Document>, Box<dyn Error>> {
        let mut processed_data = Vec::new();
        for rec in data {
            let date = NaiveDate::parse_from_str(&rec.date, "%Y-%m-%d")?;
            let time = NaiveTime::parse_from_str(&rec.time, "%H:%M:%S")?;
            let naive_datetime = NaiveDateTime::new(date, time);
 
            let datetime_utc: DateTime<Utc> = DateTime::from_utc(naive_datetime, Utc);
            let bson_datetime = Bson::DateTime(BsonDateTime::from_millis(datetime_utc.timestamp_millis()));
 
            let doc = doc! {
                "datetime": bson_datetime,
                "open": rec.open,
                "high": rec.high,
                "low": rec.low,
                "close": rec.close,
                "volume": rec.volume,
                "ticker": rec.ticker.clone(),
            };
 
            processed_data.push(doc);
        }

        let ticker_value = processed_data
                            .first()
                            .and_then(|first_doc| first_doc.get("ticker"))
                            .and_then(|ticker_bson| match ticker_bson {
                                Bson::String(ticker_str) => Some(ticker_str.clone()),
                                _ => None,
                            })
                            .unwrap_or_else(|| {
                                println!("Ticker not found or invalid");
                                "unknown_ticker".to_string()
                            });
    
        let record_doc = doc! {
            "ticker": ticker_value.to_string(),
            "data": processed_data.clone()
        };
        Ok(vec![record_doc])
    }
}

pub struct MongoCsvInserter {
    // pub file_path: String,
    pub mongo_uri: String,
    pub db_name: String,
    pub collection_name: String,
    client: Client,
    collection: Collection<Document>
}

impl MongoCsvInserter {
    pub async fn new(
                // file_path: &str,
                mongo_uri: &str,
                db_name: &str,
                collection_name: &str
            ) -> Result<Self, Box<dyn Error>> {
        let client_options = match ClientOptions::parse(mongo_uri).await {
            Ok(options) => options,
            Err(e) => {
                eprintln!("Failed to parse client options: {}", e);
                return Err(Box::new(e));
            }
        };
        let client = Client::with_options(client_options)?;
        let database = client.database(db_name);
        let collection: Collection<Document> = database.collection(collection_name);

        Self {
            // file_path: file_path.to_string(),
            mongo_uri: mongo_uri.to_string(),
            db_name: db_name.to_string(),
            collection_name: collection_name.to_string(),
        }
    }
    pub fn process_data(&self, file_path: &str) -> Result<(), Box<dyn Error>>{
        let mut rdr = ReaderBuilder::new()
                        .has_headers(true)
                        .from_path(&self.file_path);
        let csv_proc = CsvProcessor::new(file_path);
        let records = match csv_proc.read_csv(){
            Ok(records) => records,
            Err(e) => {
                eprintln!("Error Coming from `CsvProcessor::read_csv` function: {}",e);
                return Err(e);
            }
        };
        let bson_records = match csv_proc.to_records(records) {
            Ok(bson_records) => bson_records,
            Err(e) => {
                eprintln!("Error Coming from `CsvProcessor::to_records` function: {}", e);
                return Err(Box::new(e));
            }
        };

        let client_options = match ClientOptions::parse(&self.mongo_uri).await {
            Ok(options) => options,
            Err(e) => {
                eprintln!("Failed to parse client options: {}", e);
                return Err(Box::new(e));
            }
        };
        let client = Client::with_options(client_options)?;


        collection.insert_many(bson_records, None).await?;
    }
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let file_path = "/path/to/csv/files/file.csv";  // update this path accordingly
    let cp = CsvProcessor::new(file_path);
 
    let records = cp.read_csv()?;
    let bson_records = cp.to_records(records)?;
 
    let client_uri = "mongodb://localhost:27017";
    let client_options = ClientOptions::parse(client_uri).await?;
    let client = Client::with_options(client_options)?;
 
    let database = client.database("my_database");
    let collection: Collection<Document> = database.collection("my_collection");
 
    collection.insert_many(bson_records, None).await?;
 
    println!("Data successfully inserted into MongoDB.");
 
    Ok(())
}
