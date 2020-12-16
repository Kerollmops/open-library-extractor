# open-library-extractor

Extract the books and authors from [the open-library dumps](https://openlibrary.org/developers/dumps).

## Usage

Your first need to download the "all types dump" which gives you a `txt.gz` file, which is in fact a tsv.
Once this file is downloaded you can extract the informations into a nd-JSON.

```bash
cargo build --release
./target/release/open-library ../ol_dump_latest.txt.gz | gzip > books-authors.ndjson.gz
```
