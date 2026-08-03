-- Throwaway integration-test fixture: the minimal `musicbrainz`-schema subset
-- every SQL statement in src/queries.rs touches, with one seeded artist
-- (alias + tag + genre + annotation + url relation) and two releases in one
-- release group (media/tracks/recordings, statuses, dates). Applied to a
-- CREATE DATABASE'd test database only — never a real mirror.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE SCHEMA musicbrainz;

CREATE OR REPLACE FUNCTION musicbrainz.f_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$$ SELECT public.unaccent('public.unaccent', $1) $$;

CREATE TABLE musicbrainz.artist_type (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.artist (
    id        integer PRIMARY KEY,
    gid       uuid NOT NULL,
    name      text NOT NULL,
    sort_name text NOT NULL,
    comment   text NOT NULL DEFAULT '',
    type      integer
);

CREATE TABLE musicbrainz.artist_alias (
    id        integer PRIMARY KEY,
    artist    integer NOT NULL,
    name      text NOT NULL,
    sort_name text
);

CREATE TABLE musicbrainz.tag (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.artist_tag (
    artist integer NOT NULL,
    tag    integer NOT NULL,
    count  integer NOT NULL
);

CREATE TABLE musicbrainz.genre (
    id   integer PRIMARY KEY,
    gid  uuid NOT NULL,
    name text NOT NULL
);

CREATE TABLE musicbrainz.annotation (
    id      integer PRIMARY KEY,
    text    text,
    created timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE musicbrainz.artist_annotation (
    artist     integer NOT NULL,
    annotation integer NOT NULL
);

CREATE TABLE musicbrainz.link_type (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.link (
    id        integer PRIMARY KEY,
    link_type integer NOT NULL
);

CREATE TABLE musicbrainz.url (
    id  integer PRIMARY KEY,
    url text NOT NULL
);

CREATE TABLE musicbrainz.l_artist_url (
    id      integer PRIMARY KEY,
    link    integer NOT NULL,
    entity0 integer NOT NULL,
    entity1 integer NOT NULL
);

CREATE TABLE musicbrainz.artist_credit (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.artist_credit_name (
    artist_credit integer NOT NULL,
    position      smallint NOT NULL,
    artist        integer NOT NULL,
    name          text NOT NULL
);

CREATE TABLE musicbrainz.release_group_primary_type (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.release_group_secondary_type (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.release_group (
    id            integer PRIMARY KEY,
    gid           uuid NOT NULL,
    name          text NOT NULL,
    comment       text NOT NULL DEFAULT '',
    artist_credit integer NOT NULL,
    type          integer
);

CREATE TABLE musicbrainz.release_group_secondary_type_join (
    release_group  integer NOT NULL,
    secondary_type integer NOT NULL
);

CREATE TABLE musicbrainz.release_group_meta (
    id                       integer PRIMARY KEY,
    first_release_date_year  smallint,
    first_release_date_month smallint,
    first_release_date_day   smallint
);

CREATE TABLE musicbrainz.release_status (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.release (
    id            integer PRIMARY KEY,
    gid           uuid NOT NULL,
    name          text NOT NULL,
    artist_credit integer NOT NULL,
    release_group integer NOT NULL,
    status        integer,
    comment       text NOT NULL DEFAULT ''
);

CREATE TABLE musicbrainz.iso_3166_1 (
    area integer PRIMARY KEY,
    code text NOT NULL
);

CREATE TABLE musicbrainz.release_country (
    release    integer NOT NULL,
    country    integer NOT NULL,
    date_year  smallint,
    date_month smallint,
    date_day   smallint
);

CREATE TABLE musicbrainz.release_unknown_country (
    release    integer NOT NULL,
    date_year  smallint,
    date_month smallint,
    date_day   smallint
);

CREATE TABLE musicbrainz.medium_format (
    id   integer PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE musicbrainz.medium (
    id          integer PRIMARY KEY,
    release     integer NOT NULL,
    position    integer NOT NULL,
    track_count integer NOT NULL,
    name        text NOT NULL DEFAULT '',
    format      integer
);

CREATE TABLE musicbrainz.recording (
    id            integer PRIMARY KEY,
    gid           uuid NOT NULL,
    name          text NOT NULL,
    length        integer,
    artist_credit integer NOT NULL
);

CREATE TABLE musicbrainz.track (
    id            integer PRIMARY KEY,
    gid           uuid NOT NULL,
    medium        integer NOT NULL,
    position      integer NOT NULL,
    number        text NOT NULL,
    name          text NOT NULL,
    artist_credit integer NOT NULL,
    recording     integer NOT NULL
);

CREATE TABLE musicbrainz.l_release_release (
    id      integer PRIMARY KEY,
    link    integer NOT NULL,
    entity0 integer NOT NULL,
    entity1 integer NOT NULL
);

-- ── seed ──

INSERT INTO musicbrainz.artist_type (id, name) VALUES (1, 'Group');

INSERT INTO musicbrainz.artist (id, gid, name, sort_name, comment, type) VALUES
    (1, '11111111-1111-4111-8111-111111111111', 'Seaside Radio', 'Seaside Radio', 'test band', 1);

INSERT INTO musicbrainz.artist_alias (id, artist, name, sort_name) VALUES
    (1, 1, 'Régio Costera', 'Costera, Régio');

INSERT INTO musicbrainz.tag (id, name) VALUES (1, 'rock');
INSERT INTO musicbrainz.artist_tag (artist, tag, count) VALUES (1, 1, 5);
INSERT INTO musicbrainz.genre (id, gid, name) VALUES
    (1, '99999999-9999-4999-8999-999999999999', 'rock');

INSERT INTO musicbrainz.annotation (id, text, created) VALUES
    (1, 'seaside radio annotation', now());
INSERT INTO musicbrainz.artist_annotation (artist, annotation) VALUES (1, 1);

INSERT INTO musicbrainz.link_type (id, name) VALUES
    (1, 'official homepage'),
    (2, 'transl-tracklisting');
INSERT INTO musicbrainz.link (id, link_type) VALUES (1, 1), (2, 2);
INSERT INTO musicbrainz.url (id, url) VALUES (1, 'https://seaside-radio.example');
INSERT INTO musicbrainz.l_artist_url (id, link, entity0, entity1) VALUES (1, 1, 1, 1);

INSERT INTO musicbrainz.artist_credit (id, name) VALUES (1, 'Seaside Radio');
INSERT INTO musicbrainz.artist_credit_name (artist_credit, position, artist, name) VALUES
    (1, 0, 1, 'Seaside Radio');

INSERT INTO musicbrainz.release_group_primary_type (id, name) VALUES (1, 'Album');
INSERT INTO musicbrainz.release_group_secondary_type (id, name) VALUES (1, 'Live');
INSERT INTO musicbrainz.release_group (id, gid, name, comment, artist_credit, type) VALUES
    (1, '22222222-2222-4222-8222-222222222222', 'Harbour Lights', 'rg comment', 1, 1);
INSERT INTO musicbrainz.release_group_secondary_type_join (release_group, secondary_type) VALUES
    (1, 1);
INSERT INTO musicbrainz.release_group_meta
    (id, first_release_date_year, first_release_date_month, first_release_date_day) VALUES
    (1, 1997, 5, 21);

INSERT INTO musicbrainz.release_status (id, name) VALUES (1, 'Official');
INSERT INTO musicbrainz.release (id, gid, name, artist_credit, release_group, status, comment)
VALUES
    (1, '33333333-3333-4333-8333-333333333333', 'Harbour Lights', 1, 1, 1, 'deluxe edition'),
    (2, '44444444-4444-4444-8444-444444444444', 'Harbour Lights', 1, 1, 1, '');

INSERT INTO musicbrainz.iso_3166_1 (area, code) VALUES (1, 'GB');
INSERT INTO musicbrainz.release_country (release, country, date_year, date_month, date_day) VALUES
    (1, 1, 1997, 5, 21);
INSERT INTO musicbrainz.release_unknown_country (release, date_year, date_month, date_day) VALUES
    (2, 1998, NULL, NULL);

INSERT INTO musicbrainz.medium_format (id, name) VALUES (1, 'CD');
INSERT INTO musicbrainz.medium (id, release, position, track_count, name, format) VALUES
    (1, 1, 1, 2, '', 1);

INSERT INTO musicbrainz.recording (id, gid, name, length, artist_credit) VALUES
    (1, '55555555-5555-4555-8555-555555555555', 'Foghorn Morning', 284000, 1),
    (2, '66666666-6666-4666-8666-666666666666', 'Lighthouse Keeper', 387000, 1);

INSERT INTO musicbrainz.track (id, gid, medium, position, number, name, artist_credit, recording)
VALUES
    (1, '77777777-7777-4777-8777-777777777777', 1, 1, '1', 'Foghorn Morning', 1, 1),
    (2, '88888888-8888-4888-8888-888888888888', 1, 2, '2', 'Lighthouse Keeper', 1, 2);

INSERT INTO musicbrainz.l_release_release (id, link, entity0, entity1) VALUES (1, 2, 1, 2);
