use std::net::TcpStream;
use std::io::{Read, Write};
use ssh2::Session;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::SystemTime;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::collections::HashMap;
//use std::fs::File;
//use std::io::{prelude::*, BufReader};
use std::str::FromStr;
use regex::Regex;
extern crate chrono;
use chrono::{Duration, Local};
use zip_extensions::*;


fn main() {

    let _guard = sentry::init(("https://url.ru/777", sentry::ClientOptions {
        release: sentry::release_name!(),
        traces_sample_rate: 0.2, //send 20% of transaction to sentry
        ..Default::default()
    }));
    
    let name = hostname::get().unwrap();
    let hostname = name.to_string_lossy();
    let mut metrics_map:HashMap<String, i32> = HashMap::new();


    sentry::configure_scope(|scope|{
       scope.set_user(Some(sentry::User{
        id:Some(111.to_string()),
        email:Some("dima@yandex.ru".to_owned()),
        username:Some("root".to_owned()),
       ..Default::default()
       }));
       
       scope.set_tag("clearing stat", &hostname);
       });
       

       let tx_ctx = sentry::TransactionContext::new(
        &hostname,
        "main transaction",
       );

       let transaction = sentry::start_transaction(tx_ctx);

         //sentry::capture_message("This is recorded as a warning now", sentry::Level::Warning);
       sentry::capture_message("Im start!", sentry::Level::Info);

    


    const ARR_SIZE: usize = 3;
    let arr_host: [&str; ARR_SIZE] = ["localhost:22", "localhost:22", "localhost:22"];
    let arr_user: [&str; ARR_SIZE] = ["user", "user", "user"];
    let arr_pass: [&str; ARR_SIZE] = ["", "", ""];
    let arr_path: [&str; ARR_SIZE] = ["path/mnt/data/", "path/mnt/data/", "path/mnt/data/"];

    let current_day = Local::now();
    let yester_day = current_day - Duration::days(1);
    let yester_day =  yester_day.format("%Y-%m-%d");
    let yester_day =  yester_day.to_string();
    let log_file = "log.".to_owned() + &yester_day;
    let log_file_zip = "log.".to_owned() + &yester_day + ".zip";
    let log_file_path = "/app/ftpUpload/logs/".to_owned() + log_file_zip.as_str();
    let result_file = "/mnt/data/tmp/cliring.csv";
    let metrics_file = "/tmp/influx_metrics.csv";
    let result_upload = "/opt/clr/skroam/log/mediation_".to_owned() + &yester_day + ".csv";

    if Path::new(result_file).exists() {
        fs::remove_file(result_file).unwrap();
      }
    let _ = File::create(result_file);
    fs::set_permissions(result_file, fs::Permissions::from_mode(0o777)).unwrap();

    if Path::new(metrics_file).exists() {
        fs::remove_file(metrics_file).unwrap();
      }
    let _ = File::create(metrics_file);
    fs::set_permissions(metrics_file, fs::Permissions::from_mode(0o777)).unwrap();

    let span_ftp = transaction.start_child("get ftp", "download log from ftpuploader");

    for i in 0..arr_host.len(){
        println!("connect to host: {}, user: {}, path: {}", arr_host[i], arr_user[i], arr_path[i]);

    let pattern = r"^(.*)( WARN.*)(".to_owned() + &arr_path[i] + ")(.*)(\' sz )(.*)";
    let regex = Regex::new(&pattern).unwrap();
    let tcp = TcpStream::connect(arr_host[i]).unwrap();
    let mut sess = Session::new().unwrap();
            sess.set_tcp_stream(tcp);
            sess.handshake().unwrap();
            sess.userauth_password(arr_user[i], arr_pass[i]).unwrap();
    let mut contents:Vec<u8> = Vec::new();
    let sftp = sess.sftp().unwrap();
    let mut stream = sftp.open(Path::new(&log_file_path)).unwrap();
             stream.read_to_end(&mut contents).unwrap();
    let _ = std::fs::write(r"/mnt/data/tmp/".to_owned() + arr_host[i] + ".log.zip", &contents);

    let archive_file = PathBuf::from(r"/mnt/data/tmp/".to_owned() + arr_host[i] + ".log.zip");
    let entry_path = PathBuf::from_str(&log_file).expect("error file in zip");
    let mut buffer : Vec<u8> = vec![];
    let _ = zip_extract_file_to_memory(&archive_file, &entry_path, &mut buffer);
    let log = match std::str::from_utf8(&buffer) {
            Ok(v) => v,
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };
    
        for line in log.lines() {
           
            if let Some(captures) = regex.captures(&line) {
                if let (Some(fdate), Some(fname), Some(fsize)) = (captures.get(1), captures.get(4), captures.get(6)) {
                   let result_string = arr_host[i].to_owned() + ";" + fdate.into() + ";" + fname.into() + ";" + fsize.into();
                   
                   metrics_map.entry(arr_host[i].to_string()).and_modify(|count| *count += 1).or_insert(1);
                   
                  // println!("{};{};{};{}",arr_host[i], fdate.as_str(), fname.as_str(), fsize.as_str());
                   //println!("{}", result_string.as_str());

                   let mut result_file = OpenOptions::new().write(true).append(true).open(result_file).unwrap();
            if let Err(e) = writeln!(result_file, "{}", result_string) {
                    eprintln!("Couldn't write to file: {}", e);
                }
        

       


                }
            }

           


        }
        
    

    }

    span_ftp.finish();  


//отправка файла с результатом
println!("send result file...");
let span_upload = transaction.start_child("upload", "upload result file to cliring");
let tcp = TcpStream::connect("host:22").unwrap();
let mut sess = Session::new().unwrap();
        sess.set_tcp_stream(tcp);
        sess.set_compress(true);
        sess.timeout();
        sess.set_timeout(3000);
        sess.handshake().unwrap();
        sess.userauth_password("user", "pass").unwrap();
 
 let sftp = sess.sftp().unwrap();
 let mut local_file = File::open(result_file).expect("no file found");  
 let mut buffer:Vec<u8> = Vec::new();
 let _ :u64 = local_file.read_to_end(&mut buffer).unwrap().try_into().unwrap();

sftp.create(&Path::new(&result_upload))
.unwrap()
.write_all(&buffer)
.unwrap();

//save metrics
let mut metrics_file = OpenOptions::new().write(true).append(true).open(metrics_file).unwrap();
let duration_since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
let timestamp_nanos = duration_since_epoch.as_nanos(); // u128

if metrics_map.is_empty()
{
    let metrics_string = "cliring_log,srchost=localhost ok=0".to_owned() + " " + &timestamp_nanos.to_string();
    
    if let Err(e) = writeln!(metrics_file, "{}", metrics_string) {
        eprintln!("Couldn't write to file: {}", e);
    }
}
else
{    
    for (host, count) in &metrics_map {
        let metrics_string = "cliring_log,srchost=".to_owned() + host + " ok=" + &count.to_string() + " " + &timestamp_nanos.to_string();
       
        if let Err(e) = writeln!(metrics_file, "{}", metrics_string) {
            eprintln!("Couldn't write to file: {}", e);
        }

    }
}







span_upload.finish();
transaction.finish();

}

