//! Real query *shapes* emitted by the frameworks ePHPm targets.
//!
//! These are the queries that actually hit litewire in production — WordPress
//! core (wpdb), Laravel's query builder / Eloquent, Doctrine DBAL, CakePHP,
//! and legacy hand-rolled SQL (Sendy). They are mostly MySQL, because that is
//! the wire protocol those stacks speak to ePHPm; a few PostgreSQL shapes are
//! included for Laravel's `pgsql` driver.
//!
//! Where a shape has a deterministic answer it is diffed; the rest are
//! coverage (does the real query translate and execute at all).

use super::Corpus;
use crate::model::Diff;

const WP_OPTIONS: &[&str] = &[
    "CREATE TABLE wp_options (option_id INTEGER PRIMARY KEY, option_name VARCHAR(191), \
     option_value TEXT, autoload VARCHAR(20))",
    "INSERT INTO wp_options (option_id, option_name, option_value, autoload) \
     VALUES (1, 'siteurl', 'http://example.test', 'yes')",
    "INSERT INTO wp_options (option_id, option_name, option_value, autoload) \
     VALUES (2, 'blogname', 'My Site', 'yes')",
    "INSERT INTO wp_options (option_id, option_name, option_value, autoload) \
     VALUES (3, 'cron', 'a:0:{}', 'no')",
];

const WP_POSTS: &[&str] = &[
    "CREATE TABLE wp_posts (ID INTEGER PRIMARY KEY, post_status VARCHAR(20), \
     post_type VARCHAR(20), post_date TEXT)",
    "INSERT INTO wp_posts (ID, post_status, post_type, post_date) \
     VALUES (1, 'publish', 'post', '2024-01-01 10:00:00')",
    "INSERT INTO wp_posts (ID, post_status, post_type, post_date) \
     VALUES (2, 'publish', 'post', '2024-01-02 10:00:00')",
    "INSERT INTO wp_posts (ID, post_status, post_type, post_date) \
     VALUES (3, 'draft', 'post', '2024-01-03 10:00:00')",
];

const APP_USERS: &[&str] = &[
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), \
     active INTEGER, created_at TEXT)",
    "INSERT INTO users (id, name, email, active, created_at) \
     VALUES (1, 'Alice', 'alice@example.test', 1, '2024-01-01 00:00:00')",
    "INSERT INTO users (id, name, email, active, created_at) \
     VALUES (2, 'Bob', 'bob@example.test', 0, '2024-01-02 00:00:00')",
    "INSERT INTO users (id, name, email, active, created_at) \
     VALUES (3, 'Carol', 'carol@example.test', 1, '2024-01-03 00:00:00')",
];

pub(super) fn add(c: &mut Corpus<'_>) {
    wordpress(c);
    laravel(c);
    doctrine(c);
    cakephp(c);
    sendy(c);
}

fn wordpress(c: &mut Corpus<'_>) {
    c.my(
        "framework:wordpress",
        "options_autoload",
        WP_OPTIONS,
        "SELECT option_name, option_value FROM wp_options WHERE autoload = 'yes' \
         ORDER BY option_name",
        Diff::OrderedRows,
    );
    c.my(
        "framework:wordpress",
        "get_option",
        WP_OPTIONS,
        "SELECT option_value FROM wp_options WHERE option_name = 'blogname' LIMIT 1",
        Diff::Rows,
    );
    // update_option upsert path.
    c.my(
        "framework:wordpress",
        "update_option_upsert",
        &[
            "CREATE TABLE wp_options (option_id INTEGER PRIMARY KEY, option_name VARCHAR(191), \
             option_value TEXT, autoload VARCHAR(20))",
            "INSERT INTO wp_options (option_id, option_name, option_value, autoload) \
             VALUES (1, 'blogname', 'Old', 'yes')",
            "INSERT INTO wp_options (option_id, option_name, option_value, autoload) \
             VALUES (1, 'blogname', 'New', 'yes') ON DUPLICATE KEY UPDATE option_value = 'New'",
        ],
        "SELECT option_value FROM wp_options WHERE option_name = 'blogname'",
        Diff::Rows,
    );
    // The main loop query: SQL_CALC_FOUND_ROWS + LIMIT with offset. The
    // returned set is diffable (the hint changes nothing about which rows come
    // back); FOUND_ROWS() is checked separately below.
    c.my(
        "framework:wordpress",
        "main_query_calc_found_rows",
        WP_POSTS,
        "SELECT SQL_CALC_FOUND_ROWS ID FROM wp_posts WHERE post_status = 'publish' \
         ORDER BY post_date DESC LIMIT 0, 10",
        Diff::OrderedRows,
    );
    c.my(
        "framework:wordpress",
        "found_rows",
        &[
            "CREATE TABLE wp_posts (ID INTEGER PRIMARY KEY, post_status VARCHAR(20))",
            "INSERT INTO wp_posts (ID, post_status) VALUES (1, 'publish')",
            "INSERT INTO wp_posts (ID, post_status) VALUES (2, 'publish')",
            "INSERT INTO wp_posts (ID, post_status) VALUES (3, 'publish')",
            "SELECT SQL_CALC_FOUND_ROWS ID FROM wp_posts WHERE post_status = 'publish' LIMIT 2",
        ],
        "SELECT FOUND_ROWS()",
        Diff::Rows,
    );
    // Real WordPress schema fragment: BIGINT auto-increment + prefix-length
    // index. Coverage (DDL).
    c.my(
        "framework:wordpress",
        "posts_schema",
        &[],
        "CREATE TABLE wp_postmeta (meta_id BIGINT PRIMARY KEY, post_id BIGINT, \
         meta_key VARCHAR(255), meta_value TEXT, KEY post_id (post_id), \
         KEY meta_key (meta_key(191)))",
        Diff::None,
    );
}

fn laravel(c: &mut Corpus<'_>) {
    c.my(
        "framework:laravel",
        "find_by_email",
        APP_USERS,
        "SELECT `id`, `name` FROM `users` WHERE `email` = 'alice@example.test' LIMIT 1",
        Diff::Rows,
    );
    c.my(
        "framework:laravel",
        "find_by_pk",
        APP_USERS,
        "SELECT `id`, `name` FROM `users` WHERE `users`.`id` = 2 LIMIT 1",
        Diff::Rows,
    );
    c.my(
        "framework:laravel",
        "count_aggregate",
        APP_USERS,
        "SELECT COUNT(*) AS aggregate FROM `users`",
        Diff::Rows,
    );
    c.my(
        "framework:laravel",
        "paginate",
        APP_USERS,
        "SELECT `id` FROM `users` ORDER BY `created_at` DESC LIMIT 2 OFFSET 0",
        Diff::OrderedRows,
    );
    c.my(
        "framework:laravel",
        "where_in_order",
        APP_USERS,
        "SELECT `name` FROM `users` WHERE `id` IN (1, 3) ORDER BY `name` ASC",
        Diff::OrderedRows,
    );
    c.my(
        "framework:laravel",
        "exists_probe",
        APP_USERS,
        "SELECT EXISTS(SELECT * FROM `users` WHERE `id` = 1) AS `exists`",
        Diff::Rows,
    );
    // PostgreSQL driver shapes.
    c.pg(
        "framework:laravel",
        "pg_find_by_email",
        APP_USERS,
        "SELECT \"id\", \"name\" FROM \"users\" WHERE \"email\" = 'alice@example.test' LIMIT 1",
        Diff::Rows,
    );
    c.pg(
        "framework:laravel",
        "pg_count_aggregate",
        APP_USERS,
        "SELECT COUNT(*) AS aggregate FROM \"users\"",
        Diff::Rows,
    );
}

fn doctrine(c: &mut Corpus<'_>) {
    c.my(
        "framework:doctrine",
        "aliased_projection",
        APP_USERS,
        "SELECT t0.id AS id_1, t0.name AS name_2 FROM users t0 WHERE t0.id = 1",
        Diff::Rows,
    );
    c.my(
        "framework:doctrine",
        "in_list",
        APP_USERS,
        "SELECT t0.name AS name_1 FROM users t0 WHERE t0.id IN (1, 2, 3) ORDER BY t0.name ASC",
        Diff::OrderedRows,
    );
    c.pg(
        "framework:doctrine",
        "pg_aliased_projection",
        APP_USERS,
        "SELECT t0.id AS id_1, t0.name AS name_2 FROM users t0 WHERE t0.id = 1",
        Diff::Rows,
    );
}

fn cakephp(c: &mut Corpus<'_>) {
    c.my(
        "framework:cakephp",
        "aliased_select",
        APP_USERS,
        "SELECT Users.id AS Users__id, Users.name AS Users__name FROM users Users \
         WHERE Users.active = 1 ORDER BY Users.name ASC",
        Diff::OrderedRows,
    );
    c.my(
        "framework:cakephp",
        "count_alias",
        APP_USERS,
        "SELECT (COUNT(*)) AS count FROM users Users",
        Diff::Rows,
    );
}

fn sendy(c: &mut Corpus<'_>) {
    const SUBSCRIBERS: &[&str] = &[
        "CREATE TABLE subscribers (id INTEGER PRIMARY KEY, email VARCHAR(255), list INTEGER, \
         unsubscribed INTEGER)",
        "INSERT INTO subscribers (id, email, list, unsubscribed) \
         VALUES (1, 'a@x.test', 1, 0)",
        "INSERT INTO subscribers (id, email, list, unsubscribed) \
         VALUES (2, 'b@x.test', 1, 1)",
        "INSERT INTO subscribers (id, email, list, unsubscribed) \
         VALUES (3, 'c@x.test', 2, 0)",
    ];
    c.my(
        "framework:sendy",
        "lookup_by_email_list",
        SUBSCRIBERS,
        "SELECT id FROM subscribers WHERE email = 'a@x.test' AND list = 1",
        Diff::Rows,
    );
    c.my(
        "framework:sendy",
        "active_count",
        SUBSCRIBERS,
        "SELECT COUNT(*) FROM subscribers WHERE list = 1 AND unsubscribed = 0",
        Diff::Rows,
    );
    // mysqldump-style multi-row INSERT with a backslash-escaped quote inside a
    // literal. The emit-side escaping bug silently dropped the offending row
    // and every row after it; this guards that all rows survive.
    c.my(
        "framework:sendy",
        "dump_backslash_escape",
        &[
            "CREATE TABLE campaigns (id INTEGER PRIMARY KEY, title TEXT)",
            "INSERT INTO campaigns (id, title) VALUES (1, 'a\\'b'), (2, 'plain'), (3, 'last')",
        ],
        "SELECT COUNT(*) FROM campaigns",
        Diff::Rows,
    );
}
