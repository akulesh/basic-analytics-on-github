#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" <<-EOSQL
  CREATE DATABASE prefect;

EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
CREATE SCHEMA ${POSTGRES_SCHEMA};

CREATE table ${POSTGRES_SCHEMA}.repo (
    id SERIAL,
    name VARCHAR(255),
    owner VARCHAR(255),
    language VARCHAR(255),
    url VARCHAR(255),
    topics VARCHAR(255),
    archived SMALLINT DEFAULT 0,
    has_license SMALLINT DEFAULT 0,
    has_description SMALLINT DEFAULT 0,
    has_topic SMALLINT DEFAULT 0,
    has_wiki SMALLINT DEFAULT 0,
    default_branch VARCHAR(255),
    license VARCHAR(255),
    description TEXT,
    size INTEGER,
    stars INTEGER,
    watchers INTEGER,
    forks INTEGER,
    open_issues INTEGER,
    days_since_creation INTEGER,
    days_since_last_commit INTEGER,
    creation_date TIMESTAMP,
    last_commit_date TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE table ${POSTGRES_SCHEMA}.repo_topic (
    id SERIAL PRIMARY KEY,
    repo_id BIGINT,
    topic VARCHAR(255),
    language VARCHAR(255),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE table ${POSTGRES_SCHEMA}.owner (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    url VARCHAR(255),
    n_repos INTEGER,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE table ${POSTGRES_SCHEMA}.repo_analytics (
    id SERIAL PRIMARY KEY,
    language VARCHAR(255),
    license VARCHAR(255),
    default_branch VARCHAR(255),
    creation_date TIMESTAMP,
    creation_year SMALLINT,
    last_commit_date TIMESTAMP,
    last_commit_year SMALLINT,
    n_repos INTEGER,
    n_owners INTEGER,
    n_archived_repos INTEGER,
    n_repos_with_license INTEGER,
    n_repos_with_topic INTEGER,
    n_repos_with_desc INTEGER,
    n_repos_with_wiki INTEGER,
    size INTEGER,
    stars NUMERIC,
    watchers NUMERIC,
    forks NUMERIC,
    open_issues NUMERIC,
    days_since_creation SMALLINT,
    days_since_last_commit SMALLINT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE table ${POSTGRES_SCHEMA}.topic_analytics (
    id SERIAL PRIMARY KEY,
    language VARCHAR(255),
    topic VARCHAR(255),
    creation_date TIMESTAMP,
    last_commit_date TIMESTAMP,
    n_repos INTEGER,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ${POSTGRES_SCHEMA}.repo_file_metadata (
    id SERIAL PRIMARY KEY,
    repo_language VARCHAR(25),
    repo_creation_date DATE,
    n_repos SMALLINT,
    file_path VARCHAR(255),
    elapsed_time_seconds INTEGER,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (repo_language, repo_creation_date)
);

EOSQL
