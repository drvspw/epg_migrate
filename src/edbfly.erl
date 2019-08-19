-module(edbfly).

%% API exports
-export([main/1]).

-include("edbfly.hrl").

-define(VSN, "1.2.0").
%%====================================================================
%% API functions
%%====================================================================
main(Args) ->
    {ok,_} = application:ensure_all_started(?APP_NAME),
    OptSpecList = option_spec_list(),
    case getopt:parse(OptSpecList, Args) of
        {ok, {[], []}} ->
            help();

        {ok, {Options, [Command]}} ->
            execute_command(Command, maps:from_list(Options));

        {ok, {_,_}} ->
            help();

        {error, {Reason, Data}} ->
            io:format("~s ~p~n", [Reason, Data]),
            help(),
            erlang:halt(1)
    end.

%%====================================================================
%% Internal functions
%%====================================================================
execute_command(?CMD_VERSION, _Options) ->
    version();

execute_command(?CMD_HELP, _Options) ->
    help();

execute_command(?CMD_MIGRATE, Options) ->
    execute_migration_command(?CMD_MIGRATE, Options);

execute_command(?CMD_LIST, Options) ->
    execute_migration_command(?CMD_LIST, Options);

execute_command(_,__) ->
    help().

execute_migration_command(Command, Options) ->
    try
        Config = parse_options(Options),
        edbfly_cmd:exec(Command, Config)
    catch
        _ : Reason : StackTrace ->
            io:format("ERROR: ~p~n", [Reason]),
            erlang:display(StackTrace),
            erlang:halt(1)
    end.

parse_options(Options) ->
    #{dir := SqlDir, config := ConfigFile} = Options,

    %% Set dir in env
    ok = application:set_env(?APP_NAME, ?APP_ENV_SQL_DIR, SqlDir),

    %% Parse yml. Verify there is only one db configuration
    [Mappings] = yamerl_constr:file(ConfigFile),

    %% Get Db Config
    [DbConfig] = Mappings,

    maps:from_list(DbConfig).

option_spec_list() ->
    ConfigHelp = "Provide the path to yml configuration file. "
               "When none is provided edbfly checks if there's "
               "an edbfly.yml file in the current directory.",

    DirHelp = "Provide the path to the directory that contains migration sql files. "
               "When none is provided edbfly checks for sql files in the current directory.",

    [
     {config, $c, "config", {string, "edbfly.yml"}, ConfigHelp},
     {dir, $d, "dir", {string, "./"}, DirHelp}

    ].

help() ->
    %% Print version
    version(),

    %% Print usage
    OptSpecList = option_spec_list(),
    getopt:usage(OptSpecList, atom_to_list(?APP_NAME), "migrate|list", []).


version() ->
    io:format("Version: ~s\n", [?VSN]).
