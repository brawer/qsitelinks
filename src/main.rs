// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT
//
// Build a mapping from Wikimedia page titles to Wikidata IDs.
// The output file is a zstd-compressed LMDB database that maps
// "en:page_title" --> "Q1234". The keys are case-folded according
// to the Unicode case folding algorithm, with the Unicode-provided
// special mapping for Turkic languages. The keys also include
// Wikipedia sister projects such as "en.wikisource:foo_bar".
//
// TODO: Currently, the mapping only uses *current* page titles.
// We should also incorporate data about redirects from pages
// that formerly existed. This will substantially grow the size
// of the output data file, but make the mapping more reliable.
//
// TODO: Currently, we do not compact the LMDB database which
// wastes several gigabytes of storage. To fix this, the Rust
// LMDB wrapper needs to include a binding for the mdb_copy.

use bzip2::read::MultiBzDecoder;
use clap::Parser;
use lmdb;
use lmdb::Transaction;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use unicode_casefold::{Locale, UnicodeCaseFold, Variant};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to wikidata dump file in json.bz2 format
    wikidata_dump: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut env_flags = lmdb::EnvironmentFlags::empty();
    env_flags.set(lmdb::EnvironmentFlags::NO_SUB_DIR, true);
    env_flags.set(lmdb::EnvironmentFlags::WRITE_MAP, true);

    let env_path = PathBuf::from("qsitelinks.mdb");
    let output_path = PathBuf::from("qsitelinks.mdb.zst");
    let env = lmdb::Environment::new()
        .set_flags(env_flags)
        .set_map_size(8 * 1024 * 1024 * 1024)
        .set_max_dbs(1)
        .open(&env_path)
        .expect("cannot create LMDB environment");
    let db = env.create_db(None, lmdb::DatabaseFlags::empty())?;
    process(&args.wikidata_dump, &env, &db)?;
    compress(&env_path, &output_path)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Entity {
    id: String,
    sitelinks: BTreeMap<String, Sitelink>,
}

#[derive(Debug, Deserialize)]
struct Sitelink {
    title: String,
}

fn process(
    dump: &PathBuf,
    env: &lmdb::Environment,
    db: &lmdb::Database,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(dump)?;
    let decompressor = MultiBzDecoder::new(file);
    let reader = BufReader::new(decompressor);
    let mut txn = env.begin_rw_txn().unwrap();
    let mut num_lines = 0u64;
    let now = SystemTime::now();
    for maybe_line in reader.lines() {
        let mut line = maybe_line?;
        if line.len() < 5 {
            continue;
        }
        if line.ends_with(",") {
            line.pop();
        }
        num_lines += 1;
        if num_lines % 100_000 == 1 {
            if let Ok(elapsed) = now.elapsed() {
                println!(
                    "processed {} entities in {}s",
                    num_lines,
                    elapsed.as_secs_f32()
                );
            }
        }
        if true && num_lines > 100 {
            break;
        }
        let e: serde_json::Result<Entity> = serde_json::from_str(&line);
        if e.is_err() {
            continue;
        }
        let e = e.unwrap();
        if e.sitelinks.is_empty() {
            continue;
        }
        {
            for (key, p) in e.sitelinks {
                let mut iter = key.split("wiki");
                if let Some(mut lang) = iter.next() {
                    if lang.is_empty() {
                        lang = "und";
                    }
                    if let Some(mut site) = iter.next() {
                        if site.is_empty() && (lang == "commons" || lang == "species") {
                            site = lang;
                            lang = "und";
                        }
                        let key = make_key(lang, site, &p.title);
                        txn.put(*db, &key, &e.id, lmdb::WriteFlags::empty())?;
                    }
                }
            }
        }
    }
    txn.commit()?;
    Ok(())
}

fn make_key(lang: &str, site: &str, title: &str) -> String {
    let cap = lang.len() + 1 + site.len() + 1 + title.len();
    let mut s = String::with_capacity(cap);
    s.push_str(lang);
    if !site.is_empty() {
        s.push_str(".wiki");
        s.push_str(site);
    }
    s.push(':');

    // https://en.wikipedia.org/wiki/List_of_Turkic_languages
    let locale = match lang {
        "aib" => Locale::Turkic, // Äynu
        "alt" => Locale::Turkic, // Southern Altai
        "atv" => Locale::Turkic, // Northern Altai
        "az" => Locale::Turkic,  // Azerbaijani
        "ba" => Locale::Turkic,  // Bashkir
        "chg" => Locale::Turkic, // Chagatai
        "cjs" => Locale::Turkic, // Shor
        "clw" => Locale::Turkic, // Chulym
        "crh" => Locale::Turkic, // Crimean Tatar
        "cv" => Locale::Turkic,  // Chuvash
        "dlg" => Locale::Turkic, // Dolgan
        "gag" => Locale::Turkic, // Gagauz
        "ili" => Locale::Turkic, // Ili Turki
        "jct" => Locale::Turkic, // Krymchak
        "kaa" => Locale::Turkic, // Karakalpak
        "kdr" => Locale::Turkic, // Karaim
        "kim" => Locale::Turkic, // Tofa
        "kjh" => Locale::Turkic, // Khakas
        "kk" => Locale::Turkic,  // Kazakh
        "klj" => Locale::Turkic, // Khalaj
        "kmz" => Locale::Turkic, // Khorasani Turkic
        "krc" => Locale::Turkic, // Karachay-Balkar
        "kum" => Locale::Turkic, // Kumyk
        "ky" => Locale::Turkic,  // Kyrgyz
        "nog" => Locale::Turkic, // Nogai
        "ota" => Locale::Turkic, // Ottoman Turkish
        "otk" => Locale::Turkic, // Orkhon Turkic
        "oui" => Locale::Turkic, // Old Uyghur
        "qwm" => Locale::Turkic, // Kipchak
        "qxq" => Locale::Turkic, // Qashqai
        "sah" => Locale::Turkic, // Yakut
        "slq" => Locale::Turkic, // Salchuq
        "sty" => Locale::Turkic, // Siberian Tatar
        "tk" => Locale::Turkic,  // Turkmen
        "tr" => Locale::Turkic,  // Turkish
        "tt" => Locale::Turkic,  // Tatar
        "tyv" => Locale::Turkic, // Tuvan
        "ug" => Locale::Turkic,  // Uyghur
        "uum" => Locale::Turkic, // Urum
        "uz" => Locale::Turkic,  // Uzbek
        "xbo" => Locale::Turkic, // Bulgar
        "xpc" => Locale::Turkic, // Pecheneg
        "xqa" => Locale::Turkic, // Middle Turkic
        "ybe" => Locale::Turkic, // Western Yugur
        "zkh" => Locale::Turkic, // Khorezmian
        "zkz" => Locale::Turkic, // Khazar
        _ => Locale::NonTurkic,
    };
    for c in title.case_fold_with(Variant::Full, locale) {
        if c.is_control() || c.is_whitespace() {
            s.push('_');
        } else {
            s.push(c);
        }
    }
    return s;
}

fn compress(db_path: &Path, out_path: &Path) -> Result<(), Box<dyn Error>> {
    let in_file = File::open(db_path)?;
    let out_file = File::create(out_path)?;
    zstd::stream::copy_encode(in_file, out_file, 11)?;
    Ok(())
}
