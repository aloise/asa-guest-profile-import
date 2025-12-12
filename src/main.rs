// Cargo.toml (add)
/// quick-xml = "0.29"
/// rayon = "1.7"
/// csv = "1.1"
/// anyhow = "1.0"
/// indicatif = "0.17"
/// crossbeam-channel = "0.5"

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use anyhow::{Context, Result};
use quick_xml::Reader;
use quick_xml::events::Event;
use rayon::prelude::*;
use csv::Writer;
use indicatif::{ProgressBar, ProgressStyle};
use crossbeam_channel::unbounded;

#[derive(Debug, serde::Serialize)]
struct Guest {
    id: Option<String>,
    name1: Option<String>,
    name2: Option<String>,
    geburtsdatum: Option<String>,
    geschlecht: Option<String>,
    strasse1: Option<String>,
    ort: Option<String>,
    plz: Option<String>,
    land: Option<String>,
    staatsbuergerschaft: Option<String>,
    vip: Option<String>,
    creation_time: Option<String>,
    last_update: Option<String>,
    primary_email: Option<String>,
    primary_phone: Option<String>,
}

fn parse_guests_from_file(path: &PathBuf) -> Result<Vec<Guest>> {
    let f = File::open(path).with_context(|| format!("open {:?}", path))?;
    let mut reader = Reader::from_reader(BufReader::new(f));
    reader.trim_text(true);
    let mut buf = Vec::new();

    let mut guests = Vec::new();
    let mut current_guest: Option<Guest> = None;
    let mut current_tag: Option<String> = None;

    // temp storage for communication entries inside a guest
    let mut comm_type: Option<String> = None;
    let mut comm_value: Option<String> = None;
    // collect found emails/phones
    let mut emails: Vec<String> = Vec::new();
    let mut phones: Vec<String> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match name.as_str() {
                    "Gast" => {
                        current_guest = Some(Guest {
                            id: None,
                            name1: None,
                            name2: None,
                            geburtsdatum: None,
                            geschlecht: None,
                            strasse1: None,
                            ort: None,
                            plz: None,
                            land: None,
                            staatsbuergerschaft: None,
                            vip: None,
                            creation_time: None,
                            last_update: None,
                            primary_email: None,
                            primary_phone: None,
                        });
                        emails.clear();
                        phones.clear();
                    }
                    "Kommunikation" => {
                        // start of a communication entry
                        comm_type = None;
                        comm_value = None;
                    }
                    other => {
                        // regular tag inside Gast or Kommunikation
                        current_tag = Some(other.to_string());
                    }
                }
            }
            Ok(Event::Text(e)) => {
                let txt = e.unescape().unwrap_or_default().into_owned();
                if let Some(tag) = current_tag.take() {
                    if let Some(g) = current_guest.as_mut() {
                        match tag.as_str() {
                            "ID" => g.id = Some(txt),
                            "Name1" => g.name1 = Some(txt),
                            "Name2" => g.name2 = Some(txt),
                            "Geburtsdatum" => g.geburtsdatum = Some(txt),
                            "Geschlecht" => g.geschlecht = Some(txt),
                            "Strasse1" => g.strasse1 = Some(txt),
                            "Ort" => g.ort = Some(txt),
                            "PLZ" => g.plz = Some(txt),
                            "Land" => g.land = Some(txt),
                            "Staatsbuergerschaft" => g.staatsbuergerschaft = Some(txt),
                            "VIP" => g.vip = Some(txt),
                            "CreationTime" => g.creation_time = Some(txt),
                            "LastUpdateTime" => g.last_update = Some(txt),
                            // communication sub-tags captured below
                            "Typ" => {
                                comm_type = Some(txt);
                            }
                            "NummerAdresse" => {
                                comm_value = Some(txt);
                            }
                            _ => { /* ignore other tags */ }
                        }
                    } else {
                        // if not inside a guest, ignore
                    }
                } else {
                    // sometimes Kommmunikation child nodes appear without current_tag
                    // handle Typ / NummerAdresse by checking last tag from reader isn't available here
                }

                // If both comm_type and comm_value are set, push to lists and clear
                if let (Some(t), Some(v)) = (comm_type.take(), comm_value.take()) {
                    let t_lower = t.to_lowercase();
                    if t_lower.starts_with('e') { // E1, E2 -> email
                        emails.push(v.clone());
                    } else {
                        // treat others as phone/fax/mobile
                        phones.push(v.clone());
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "Gast" {
                    if let Some(mut g) = current_guest.take() {
                        // choose primary email and phone by preference
                        g.primary_email = emails.get(0).cloned();
                        g.primary_phone = phones.get(0).cloned();
                        guests.push(g);
                    }
                }
                // reset tag on any end
                current_tag = None;
            }
            Ok(Event::Eof) => break,
            Err(err) => {
                return Err(anyhow::anyhow!("Error parsing {:?}: {}", path, err));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(guests)
}

fn main() -> Result<()> {
    let dir = "/Users/imordashev/workspace/smart-host/docs/adler-resort-sicilia/adler-resort-sicilia/pull_profiles"; // change
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|d| d.path()))
        .filter(|p| p.extension().map(|s| s == "xml").unwrap_or(false))
        .collect();

    paths.sort();

    let pb = ProgressBar::new(paths.len() as u64);
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let (tx, rx) = unbounded::<Guest>();

    // CSV writer thread
    let writer_handle = std::thread::spawn(move || -> Result<()> {
        let mut wtr = Writer::from_path("guests.csv")?;
        for guest in rx.iter() {
            wtr.serialize(guest)?;
        }
        wtr.flush()?;
        Ok(())
    });

    // adjust number of threads if IO bound
    paths.par_iter().for_each_with(tx.clone(), |sender, path| {
        match parse_guests_from_file(path) {
            Ok(list) => {
                for g in list {
                    let _ = sender.send(g);
                }
            }
            Err(err) => {
                eprintln!("Failed to parse {:?}: {}", path, err);
            }
        }
        pb.inc(1);
    });

    drop(tx);
    pb.finish_with_message("done");
    writer_handle.join().expect("writer thread")?;
    Ok(())
}