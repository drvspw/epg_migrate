-ifndef('__edbfly_hrl__').
-define('__edbfly_hrl__', true).

-define(APP_NAME, edbfly).

-define(CMD_MIGRATE, "migrate").
-define(CMD_LIST, "list").
-define(CMD_VERSION, "version").
-define(CMD_HELP, "help").

-define(APP_ENV_SQL_DIR, sql_dir).
-define(APP_ENV_MIGRATIONS, migrations).

-define(DEFAULT_SCHEMA, "public").

-define(MUTEX_GRAB_FAIL, <<"55P03">>).
-define(DUPLICATE_TABLE, <<"42P07">>).

-define(SET_SCHEMA_QUERY(Schema), iolist_to_binary(["SET SCHEMA '", Schema, "'"])).
-define(LOCK_TABLE_QUERY, <<"LOCK TABLE migrations IN ACCESS EXCLUSIVE MODE NOWAIT">>).

-define(CREATE_MIGRATIONS_TABLE_QUERY, <<"CREATE TABLE IF NOT EXISTS migrations(name VARCHAR(255) PRIMARY KEY, status VARCHAR(255), time TIMESTAMPTZ)">>).

-define(LIST_MIGRATIONS_QUERY, <<"SELECT name, status, time FROM migrations">>).
-define(SELECT_MIGRATION_QUERY, iolist_to_binary([?LIST_MIGRATIONS_QUERY, <<" WHERE name = $1">>])).
-define(INSERT_MIGRATION_QUERY, <<"INSERT INTO migrations (name,status,time) VALUES($1, 'DONE', now() at time zone 'utc')">>).

-endif.
