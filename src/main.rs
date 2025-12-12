use anyhow::{Context, Result};
use crossbeam_channel::unbounded;
use csv::Writer;
use dashmap::DashSet;
use indicatif::{ProgressBar, ProgressStyle};
use quick_xml::Reader;
use quick_xml::events::Event;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

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
    newsletter_agreement: bool,
}

fn parse_guests_from_file(path: &PathBuf) -> Result<Vec<Guest>> {
    let f = File::open(path).with_context(|| format!("open {:?}", path))?;
    let mut reader = Reader::from_reader(BufReader::new(f));
    // reader.trim_text(true);
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
                            newsletter_agreement: false,
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
                let txt = e.decode().unwrap_or_default().into_owned();
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
                            "Newsletter" => {
                                // ignore for now
                                g.newsletter_agreement = txt == "true";
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
            }

            // Inside the loop, replace the Event::End arm with this:
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "Kommunikation" {
                    if let (Some(t), Some(v)) = (comm_type.take(), comm_value.take()) {
                        let t_lower = t.to_lowercase();
                        if t_lower.starts_with('e') {
                            emails.push(v.clone());
                        } else {
                            phones.push(v.clone());
                        }
                    }
                }
                if name == "Gast" {
                    if let Some(mut g) = current_guest.take() {
                        g.primary_email = emails.get(0).cloned();
                        g.primary_phone = phones.get(0).cloned();
                        guests.push(g);
                    }
                }
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
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}",
        )?
        .progress_chars("#>-"),
    );

    let (tx, rx) = unbounded::<Guest>();

    // CSV writer thread
    let writer_handle = std::thread::spawn(move || -> Result<()> {
        let mut wtr = Writer::from_path("./guests-with-agreement.csv")?;
        for guest in rx.iter() {
            wtr.serialize(guest)?;
        }
        wtr.flush()?;
        Ok(())
    });

    let seen_emails = Arc::new(DashSet::new());

    // adjust number of threads if IO bound
    paths.par_iter().for_each_with(tx.clone(), |sender, path| {
        match parse_guests_from_file(path) {
            Ok(list) => {
                for g in list {
                    if g.newsletter_agreement {
                        if let Some(email) = &g.primary_email {
                            // Insert returns true if the email was not present
                            let clean_email = email.trim().to_lowercase();
                            if seen_emails.insert(clean_email) {
                                let _ = sender.send(g);
                            }
                        }
                    }
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

    println!("Unique emails count: {}", seen_emails.len());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_single_guest() {
        let xml = r#"
            <Gaste>
                <Gast>
                    <ID>1</ID>
                    <Name1>John</Name1>
                    <Name2>Doe</Name2>
                    <Geburtsdatum>1990-01-01</Geburtsdatum>
                    <Geschlecht>M</Geschlecht>
                    <Strasse1>Main Street 1</Strasse1>
                    <Ort>Sampletown</Ort>
                    <PLZ>12345</PLZ>
                    <Land>DE</Land>
                    <Staatsbuergerschaft>DE</Staatsbuergerschaft>
                    <VIP>no</VIP>
                    <CreationTime>2024-01-01T12:00:00</CreationTime>
                    <LastUpdateTime>2024-06-01T12:00:00</LastUpdateTime>
                    <Kommunikation>
                        <Typ>E1</Typ>
                        <NummerAdresse>john.doe@example.com</NummerAdresse>
                    </Kommunikation>
                    <Kommunikation>
                        <Typ>T1</Typ>
                        <NummerAdresse>+49123456789</NummerAdresse>
                    </Kommunikation>
                    <Newsletter>true</Newsletter>
                </Gast>
            </Gaste>
        "#;

        let mut tmpfile = NamedTempFile::new().unwrap();
        write!(tmpfile, "{}", xml).unwrap();
        let path = tmpfile.path().to_path_buf();

        let guests = parse_guests_from_file(&path).unwrap();
        assert_eq!(guests.len(), 1);
        let g = &guests[0];
        assert_eq!(g.id.as_deref(), Some("1"));
        assert_eq!(g.name1.as_deref(), Some("John"));
        assert_eq!(g.primary_email.as_deref(), Some("john.doe@example.com"));
        assert_eq!(g.primary_phone.as_deref(), Some("+49123456789"));
        assert!(g.newsletter_agreement);
    }
}
