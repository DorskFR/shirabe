# `imdb` database migrations

Per-database migrations for the dedicated, WRITABLE **`imdb`** database
(connected via `IMDB_DATABASE_URL`), which holds the bulk IMDb TSV mirror
(`title.basics`, `title.akas`, `title.ratings`, `name.basics`, …).

This directory is an intentional placeholder. The IMDb bulk-mirror tables land
in SHIB-5 as forward-only, idempotent migrations (`0001_init.sql`, …) targeting
this database only. Nothing here runs against the `musicbrainz` mirror or the
`shirabe` coordination database.
