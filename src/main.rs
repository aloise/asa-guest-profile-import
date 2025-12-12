use anyhow::{Context, Result};
use crossbeam_channel::{unbounded, Sender};
use csv::Writer;
use dashmap::DashSet;
use indicatif::{ProgressBar, ProgressStyle};
use clap::Parser;
use quick_xml::events::Event;
use quick_xml::Reader;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

#[derive(Debug, Default, serde::Serialize)]
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

#[derive(Debug, Clone, Copy)]
enum Tag {
    Gast,
    Kommunikation,
    // Guest fields
    Id,
    Name1,
    Name2,
    Geburtsdatum,
    Geschlecht,
    Strasse1,
    Ort,
    Plz,
    Land,
    Staatsbuergerschaft,
    Vip,
    CreationTime,
    LastUpdateTime,
    Newsletter,
    // Kommunikation fields
    Typ,
    NummerAdresse,
    // Anything else
    Unknown,
}

fn tag_from_bytes(name: &[u8]) -> Tag {
    match name {
        b"Gast" => Tag::Gast,
        b"Kommunikation" => Tag::Kommunikation,
        b"ID" => Tag::Id,
        b"Name1" => Tag::Name1,
        b"Name2" => Tag::Name2,
        b"Geburtsdatum" => Tag::Geburtsdatum,
        b"Geschlecht" => Tag::Geschlecht,
        b"Strasse1" => Tag::Strasse1,
        b"Ort" => Tag::Ort,
        b"PLZ" => Tag::Plz,
        b"Land" => Tag::Land,
        b"Staatsbuergerschaft" => Tag::Staatsbuergerschaft,
        b"VIP" => Tag::Vip,
        b"CreationTime" => Tag::CreationTime,
        b"LastUpdateTime" => Tag::LastUpdateTime,
        b"Newsletter" => Tag::Newsletter,
        b"Typ" => Tag::Typ,
        b"NummerAdresse" => Tag::NummerAdresse,
        _ => Tag::Unknown,
    }
}

#[derive(Debug, Default)]
struct ParseState {
    current_guest: Option<Guest>,
    current_tag: Option<Tag>,
    // temp storage for communication entries inside a guest
    comm_type: Option<String>,
    comm_value: Option<String>,
    // collect found emails/phones
    emails: Vec<String>,
    phones: Vec<String>,
}

impl ParseState {
    fn on_start(&mut self, tag: Tag) {
        match tag {
            Tag::Gast => {
                self.current_guest = Some(Guest::default());
                self.emails.clear();
                self.phones.clear();
                self.current_tag = None;
            }
            Tag::Kommunikation => {
                self.comm_type = None;
                self.comm_value = None;
                self.current_tag = None;
            }
            other => {
                self.current_tag = Some(other);
            }
        }
    }

    fn on_text(&mut self, tag: Tag, txt: String) {
        let Some(g) = self.current_guest.as_mut() else {
            return;
        };

        match tag {
            Tag::Id => g.id = Some(txt),
            Tag::Name1 => g.name1 = Some(txt),
            Tag::Name2 => g.name2 = Some(txt),
            Tag::Geburtsdatum => g.geburtsdatum = Some(txt),
            Tag::Geschlecht => g.geschlecht = Some(txt),
            Tag::Strasse1 => g.strasse1 = Some(txt),
            Tag::Ort => g.ort = Some(txt),
            Tag::Plz => g.plz = Some(txt),
            Tag::Land => g.land = Some(txt),
            Tag::Staatsbuergerschaft => g.staatsbuergerschaft = Some(txt),
            Tag::Vip => g.vip = Some(txt),
            Tag::CreationTime => g.creation_time = Some(txt),
            Tag::LastUpdateTime => g.last_update = Some(txt),
            Tag::Typ => {
                self.comm_type = Some(txt);
            }
            Tag::NummerAdresse => {
                self.comm_value = Some(txt);
            }
            Tag::Newsletter => {
                g.newsletter_agreement = txt == "true";
            }
            _ => { /* ignore other tags */ }
        }
    }

    fn on_end(&mut self, tag: Tag) -> Option<Guest> {
        match tag {
            Tag::Kommunikation => {
                if let (Some(t), Some(v)) = (self.comm_type.take(), self.comm_value.take()) {
                    let t_lower = t.to_lowercase();
                    if t_lower.starts_with('e') {
                        self.emails.push(v);
                    } else {
                        self.phones.push(v);
                    }
                }
                None
            }
            Tag::Gast => {
                let mut g = self.current_guest.take()?;
                g.primary_email = self.emails.get(0).cloned();
                g.primary_phone = self.phones.get(0).cloned();
                Some(g)
            }
            _ => None,
        }
    }
}

fn parse_guests_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<Guest>> {
    let path_ref = path.as_ref();
    let f = File::open(path_ref).with_context(|| format!("open {:?}", path_ref))?;
    let mut reader = Reader::from_reader(BufReader::new(f));
    let mut buf = Vec::new();

    let mut guests = Vec::new();
    let mut state = ParseState::default();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = tag_from_bytes(e.name().as_ref());
                state.on_start(tag);
            }
            Ok(Event::Text(e)) => {
                let txt = e.decode().unwrap_or_default().into_owned();
                if let Some(tag) = state.current_tag.take() {
                    state.on_text(tag, txt);
                }
            }
            Ok(Event::End(e)) => {
                let tag = tag_from_bytes(e.name().as_ref());
                if let Some(g) = state.on_end(tag) {
                    guests.push(g);
                }
                state.current_tag = None;
            }
            Ok(Event::Eof) => break,
            Err(err) => {
                return Err(anyhow::anyhow!("Error parsing {:?}: {}", path_ref, err));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(guests)
}

fn collect_xml_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("list dir {:?}", dir))?
        .filter_map(|e| e.ok().map(|d| d.path()))
        .filter(|p| p.extension().map(|s| s == "xml").unwrap_or(false))
        .collect();
    paths.sort();
    Ok(paths)
}

fn configure_progress_bar(total: usize) -> ProgressBar {
    let pb = ProgressBar::new(total as u64);
    let _ = pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb
}

fn spawn_csv_writer(output: &Path) -> (Sender<Guest>, JoinHandle<Result<()>>) {
    let (tx, rx) = unbounded::<Guest>();
    let path = output.to_path_buf();
    let handle = std::thread::spawn(move || -> Result<()> {
        let mut wtr = Writer::from_path(path)?;
        for guest in rx.iter() {
            wtr.serialize(guest)?;
        }
        wtr.flush()?;
        Ok(())
    });
    (tx, handle)
}

fn eligible_clean_email(g: &Guest) -> Option<String> {
    if !g.newsletter_agreement {
        return None;
    }
    let email = g.primary_email.as_deref()?;
    Some(email.trim().to_lowercase())
}

fn handle_file(path: &Path, sender: &Sender<Guest>, seen_emails: &Arc<DashSet<String>>) -> Result<()> {
    let list = parse_guests_from_file(path)?;

    for g in list {
        let Some(clean_email) = eligible_clean_email(&g) else {
            continue;
        };
        if seen_emails.insert(clean_email) {
            let _ = sender.send(g);
        }
    }

    Ok(())
}

fn process_paths(paths: &[PathBuf], tx: &Sender<Guest>, seen_emails: &Arc<DashSet<String>>, pb: &ProgressBar) {
    paths.par_iter().for_each_with(tx.clone(), |sender, path| {
        if let Err(err) = handle_file(path.as_path(), sender, seen_emails) {
            eprintln!("Failed to parse {:?}: {}", path, err);
        }
        pb.inc(1);
    });
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Extract guests from ASA XML profiles and export newsletter opt-ins to CSV",
    long_about = None
)]
struct Cli {
    /// Directory containing input XML files
    #[arg(short = 'i', long = "input", value_name = "DIR", default_value = "../docs/adler-resort-sicilia/adler-resort-sicilia/pull_profiles")]
    input_dir: PathBuf,

    /// Output CSV file path
    #[arg(short = 'o', long = "output", value_name = "FILE", default_value = "./guests-with-agreement.csv")]
    output_csv: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let paths = collect_xml_files(cli.input_dir.as_path())?;
    let pb = configure_progress_bar(paths.len());

    let (tx, writer_handle) = spawn_csv_writer(cli.output_csv.as_path());
    let seen_emails = Arc::new(DashSet::new());

    process_paths(&paths, &tx, &seen_emails, &pb);

    drop(tx);
    pb.finish_with_message("done");
    writer_handle.join().expect("writer thread")?;

    println!("Unique emails count: {}", seen_emails.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
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

    #[test]
    fn test_parse_comm_entry_missing_fields_is_ignored() {
        let xml = r#"
            <Gaste>
                <Gast>
                    <ID>1</ID>
                    <Kommunikation>
                        <Typ>E1</Typ>
                    </Kommunikation>
                    <Kommunikation>
                        <NummerAdresse>john.doe@example.com</NummerAdresse>
                    </Kommunikation>
                    <Newsletter>true</Newsletter>
                </Gast>
            </Gaste>
        "#;

        let mut tmpfile = NamedTempFile::new().unwrap();
        write!(tmpfile, "{}", xml).unwrap();

        let guests = parse_guests_from_file(tmpfile.path()).unwrap();
        assert_eq!(guests.len(), 1);
        let g = &guests[0];

        // Because no single Kommunikation contains both Typ and NummerAdresse.
        assert_eq!(g.primary_email.as_deref(), None);
        assert_eq!(g.primary_phone.as_deref(), None);
        assert!(g.newsletter_agreement);
    }

    #[test]
    fn test_parse_multiple_emails_uses_first() {
        let xml = r#"
            <Gaste>
                <Gast>
                    <ID>1</ID>
                    <Kommunikation>
                        <Typ>E1</Typ>
                        <NummerAdresse>first@example.com</NummerAdresse>
                    </Kommunikation>
                    <Kommunikation>
                        <Typ>E2</Typ>
                        <NummerAdresse>second@example.com</NummerAdresse>
                    </Kommunikation>
                    <Newsletter>true</Newsletter>
                </Gast>
            </Gaste>
        "#;

        let mut tmpfile = NamedTempFile::new().unwrap();
        write!(tmpfile, "{}", xml).unwrap();

        let guests = parse_guests_from_file(tmpfile.path()).unwrap();
        assert_eq!(guests.len(), 1);
        let g = &guests[0];
        assert_eq!(g.primary_email.as_deref(), Some("first@example.com"));
    }

    #[test]
    fn test_cli_defaults() {
        let cli = Cli::try_parse_from(["asa-profile-parser"]).unwrap();
        assert_eq!(
            cli.input_dir,
            PathBuf::from("../docs/adler-resort-sicilia/adler-resort-sicilia/pull_profiles")
        );
        assert_eq!(cli.output_csv, PathBuf::from("./guests-with-agreement.csv"));
    }

    #[test]
    fn test_cli_overrides() {
        let cli = Cli::try_parse_from([
            "asa-profile-parser",
            "--input",
            "some/input/dir",
            "--output",
            "some/output.csv",
        ])
        .unwrap();

        assert_eq!(cli.input_dir, PathBuf::from("some/input/dir"));
        assert_eq!(cli.output_csv, PathBuf::from("some/output.csv"));
    }
}
