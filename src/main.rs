use std::borrow::Cow;
use std::fs::File;
use std::io::Write as _;
use std::path::Path;
use std::{env, io};

use anyhow::Context;
use csv::{ReaderBuilder, StringRecord};
use flate2::bufread::GzDecoder;
use heed::{EnvOpenOptions, types::Str};
use serde::{Serialize, Deserialize};

#[derive(Debug, Deserialize)]
struct InAuthor<'a> {
    #[serde(borrow)]
    name: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
struct InBook<'a> {
    #[serde(borrow)]
    publishers: Option<Vec<Cow<'a, str>>>,

    #[serde(borrow)]
    physical_format: Option<Cow<'a, str>>,

    #[serde(borrow)]
    subtitle: Option<Cow<'a, str>>,

    #[serde(borrow)]
    title: Cow<'a, str>,

    number_of_pages: Option<u64>,

    #[serde(borrow)]
    publish_date: Option<Cow<'a, str>>,

    authors: Option<Vec<InAuthorKey<'a>>>,

    identifiers: Option<InIdentifiers<'a>>,

    #[serde(borrow)]
    subjects: Option<Vec<Cow<'a, str>>>,
}

#[derive(Debug, Deserialize)]
struct InAuthorKey<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
struct InIdentifiers<'a> {
    #[serde(borrow)]
    goodreads: Option<Vec<Cow<'a, str>>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum OutObject<'a> {
    Book {
        id: &'a str,
        name: &'a str,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        authors: Vec<&'a str>,

        #[serde(skip_serializing_if = "Option::is_none")]
        publish_year: Option<u32>,

        #[serde(skip_serializing_if = "Option::is_none")]
        number_of_pages: Option<u64>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        subjects: Vec<Cow<'a, str>>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        goodreads: Vec<Cow<'a, str>>,
    },
    Author {
        id: &'a str,
        name: &'a str,
    },
}

fn open_file(path: impl AsRef<Path>) -> anyhow::Result<Box<dyn io::Read>> {
    let path = path.as_ref();
    let is_gzipped = path.extension().map_or(false, |e| e == "gz");
    let file = File::open(&path).with_context(|| format!("while opening {:?}", path.display()))?;
    if is_gzipped {
        Ok(Box::new(GzDecoder::new(io::BufReader::new(file))))
    } else {
        Ok(Box::new(file))
    }
}

fn main() -> anyhow::Result<()> {
    let file_path = env::args().nth(1).with_context(|| {
        format!("usage: {} ol_dump_latest.txt.gz", env::args().nth(0).unwrap())
    })?;

    let env = EnvOpenOptions::new()
        .map_size(1024 * 1024 * 1024 * 10) // 10GB
        .max_dbs(1)
        .open(tempfile::tempdir()?)?;
    let authors_ids_names = env.create_database::<Str, Str>(Some("authors-ids-names"))?;

    eprintln!("Extracting the authors ids and names");

    let reader = open_file(&file_path)?;
    let mut reader = ReaderBuilder::new().delimiter(b'\t').has_headers(true).from_reader(reader);

    let mut wtxn = env.write_txn()?;
    let mut record = StringRecord::new();
    while reader.read_record(&mut record)? {
        if let Some(author_id) = record[1].strip_prefix("/authors/") {
            if let Ok(author) = serde_json::from_str::<InAuthor>(&record[4]) {
                authors_ids_names.put(&mut wtxn, author_id, &author.name)?;
            }
        }
    }

    wtxn.commit()?;

    eprintln!("Exporting the books editions as an ndJSON...");

    let reader = open_file(&file_path)?;
    let mut reader = ReaderBuilder::new().delimiter(b'\t').has_headers(true).from_reader(reader);
    let mut writer = io::BufWriter::new(io::stdout());

    let rtxn = env.read_txn()?;
    let mut buffer = Vec::new();
    let mut record = StringRecord::new();
    while reader.read_record(&mut record)? {
        if &record[0] == "/type/edition" {
            if let Some(book_id) = record[1].strip_prefix("/books/") {
                if let Ok(book) = serde_json::from_str::<InBook>(&record[4]) {
                    let authors = book.authors.unwrap_or_default().into_iter()
                        .flat_map(|InAuthorKey { key }| {
                            let key = key.strip_prefix("/authors/")?;
                            authors_ids_names.get(&rtxn, &key).map_err(Into::into).transpose()
                        }).collect::<anyhow::Result<Vec<_>>>()?;

                    let goodreads: Vec<_> = book.identifiers.into_iter()
                        .flat_map(|InIdentifiers { goodreads }| goodreads)
                        .flatten()
                        .collect();

                    let publish_year = book.publish_date.and_then(|s| {
                        s.get(s.len() - 4..).and_then(|s| s.parse().ok())
                    });

                    let book = OutObject::Book {
                        id: book_id,
                        name: &book.title,
                        authors: authors,
                        publish_year,
                        number_of_pages: book.number_of_pages,
                        subjects: book.subjects.unwrap_or_default(),
                        goodreads,
                    };

                    buffer.clear();
                    serde_json::to_writer(&mut buffer, &book)?;
                    buffer.push(b'\n');
                    writer.write_all(&buffer)?;
                }
            }
        }
    }

    eprintln!("Exporting the authors as an ndJSON...");

    for result in authors_ids_names.iter(&rtxn)? {
        let (id, name) = result?;
        let author = OutObject::Author { id, name };
        buffer.clear();
        serde_json::to_writer(&mut buffer, &author)?;
        buffer.push(b'\n');
        writer.write_all(&buffer)?;
    }

    writer.into_inner()?;

    Ok(())
}
