-module(edbfly_cmd).

%% API exports
-export([
         exec/2
]).

-include_lib("epgsql/include/epgsql.hrl").
-include("edbfly.hrl").

%%====================================================================
%% API functions
%%====================================================================
exec(Cmd, Config) ->
    %% Create db connection
    DbOpts = get_db_opts(Config),
    #{schema := Schema} = DbOpts,
    {ok, Db} = epgsql:connect( DbOpts ),

    %% set schema
    io:format("Setting schema to ~s...", [Schema]),
    SchemaQuery = ?SET_SCHEMA_QUERY(Schema),
    {ok, _, _} = epgsql:equery(Db, SchemaQuery),
    io:format("OK~n"),

    %% Create migrations table
    io:format("Checking migrations table..."),
    {ok, _, _} = epgsql:equery(Db, ?CREATE_MIGRATIONS_TABLE_QUERY),
    io:format("OK~n"),

    exec_cmd(Db, Cmd),

    %% close db
    ok = epgsql:close(Db).

%%====================================================================
%% private functions
%%====================================================================
exec_cmd(Db, ?CMD_MIGRATE) ->
    %% Get migration files and sort them
    {ok, Path} = application:get_env(?APP_NAME, ?APP_ENV_SQL_DIR),
    Migrations = filelib:wildcard(Path ++ "/**/*.sql"),
    {ok, SortedMigrations} = sort_migrations(Migrations),

    %% migrate
    R = case run_migrations(SortedMigrations, Db) of
            ok ->
                'OK';
            _ ->
                'FAIL'
        end,
    io:format("MIGRATIONS: ~p~n", [R]);

exec_cmd(Db, ?CMD_LIST) ->
    {ok, _, R} = epgsql:equery(Db, ?LIST_MIGRATIONS_QUERY),
    lists:foreach( fun({N,S,T}) ->
                           io:format("~-50.s ~-10.s ~-50.s ~n", [N,S,qdate:format(T)])
                       end, R),
    ok = epgsql:close(Db).

get_db_opts(Config) ->
    #{"host" := H, "port" := P, "database" := DB,
      "user" := U, "password" := PWDConfig} = Config,
    S = maps:get("schema", Config, ?DEFAULT_SCHEMA),
    PWD = extract_password( maps:from_list(PWDConfig)),

    #{host => H, port => P, database => DB, username => U,
      password => PWD, timeout => 4000, schema => S}.

extract_password(#{"type" := "text", "value" := V}) ->
    V;
extract_password(#{"type" := "aws_secret", "secret_id" := SecretId, "aws_credentials" := AwsCred}) ->
    extract_password(maps:from_list(AwsCred), SecretId).

extract_password(#{"iam_ec2_role" := Role, "region" := Region}, SecretId) ->
    extract_password(#{iam_ec2_role => Role, region => Region}, SecretId);

extract_password(#{"access_key_id" := AK, "secret_access_key" := SK, "region" := R}, SecretId) ->
    extract_password(#{access_key_id => AK, secret_access_key => SK, region => R}, SecretId);

extract_password(CredentialsMap, SecretId) ->
    #{secret_string := Secret} = edbfly_aws:get_secret(CredentialsMap, SecretId),
    try
        #{<<"password">> := V} = jsone:decode(Secret),
        V
    catch
        _ : _ ->
            Secret
    end.

run_migrations(Migrations, Db) ->
    %% store migrations in env
    ok = application:set_env(?APP_NAME, ?APP_ENV_MIGRATIONS, Migrations),

    %% Execute migrations in transaction
    case epgsql:with_transaction(Db, fun migrations_transaction_fn/1) of
        Res when element(1, Res) =:= ok ->
            ok;
        {error, #error{code = ?MUTEX_GRAB_FAIL}} ->
            io:format("Unable to lock migrations table! Retrying in 5s....~n"),
            timer:sleep(5000),
            run_migrations(Migrations, Db);
        {error, #error{code = Code}} ->
             {error, epgsql_error_codename(Code)};
        O -> O
    end.


sort_migrations(Migrations) ->
    %% filter out sql files that does not match filename convention
    MigrationsWithNumbers = [ {extract_number(filename:basename(F)), F} || F <- Migrations ],
    FilterMigrations = lists:filter( fun({N, _F}) ->
                                             N =/= -1
                                     end, MigrationsWithNumbers),

    %% Sort the migration files in ascending order
    SortFn =
        fun(A, B) ->
                {ADigit, _} = A,
                {BDigit, _} = B,
                ADigit < BDigit
        end,
    SortedMigrations = lists:sort(SortFn, FilterMigrations),

    %% Extract just the filename from sorted list
    {ok, [F || {_, F} <- SortedMigrations]}.

extract_number(S) ->
    case re:split(S, ".*_(\\d+).sql") of
        [_, I, _] ->
            binary_to_integer(I);
        _ ->
            -1
    end.

migrations_transaction_fn(Db) ->
    case epgsql:equery(Db, ?LOCK_TABLE_QUERY) of
        {ok, _, _} ->
            io:format("Locking migrations Table succeded.~n"),

            %% get migrations from env
            {ok, Migrations} = application:get_env(?APP_NAME, ?APP_ENV_MIGRATIONS),

            %% loop through and run each migration
            lists:foreach( fun(M) ->
                                   run_migration(M,Db)
                           end, Migrations);

        {error, _} = E->
            E
    end.

run_migration(Migration, Db) ->
    MigrationName = extract_migration_name(Migration),

    io:format("evaluating migration ~s....", [MigrationName]),
    case has_migration_ran(MigrationName, Db) of
        true ->
            io:format("exists~n"),
            ok;
        false ->
            io:format("executing~n"),
            Queries = read_queries_from_file(Migration),
            case execute_queries(Queries,Db) of
                ok ->
                    {ok, _} = epgsql:equery(Db, ?INSERT_MIGRATION_QUERY, [MigrationName]),
                    io:format("~n");

                ignore ->
                    io:format("ignored~n");

                _ ->
                    io:format("~nerror~n")
            end,
            ok
    end.


extract_migration_name(Migration) ->
     list_to_atom(filename:basename(Migration, ".sql")).

has_migration_ran(MigrationName, Db) ->
    case epgsql:equery(Db, ?SELECT_MIGRATION_QUERY, [MigrationName]) of
        {ok, _, []} ->
            false;
        {ok, _, [_]} ->
            true
    end.


read_queries_from_file(MigrationFile) ->
    Lines = [list_to_binary(string:trim(L)) || L <- file_into_list(MigrationFile)],
    read_queries_from_lines(normal, Lines, [], []).


read_queries_from_lines(normal, [<<"--+ begin", _/binary>>|Lines], [], Queries) ->
    %% Begining of procedure
    read_queries_from_lines(procedure, Lines, [], Queries);

read_queries_from_lines(normal, [<<"--+ begin",_/binary>> = Line|_Lines], _Buffer, _Queries) ->
    %% Nested procedure definition.
    io:format("error~nNested procedure definition~n...~s~n",[Line]),
    erlang:halt(1);

read_queries_from_lines(normal, [<<"--+ end",_/binary>> = Line|_Lines], _Buffer, _Queries) ->
    %% Nested procedure definition.
    io:format("error~nEmpty procedure definition?~n...~s~n",[Line]),
    erlang:halt(1);

read_queries_from_lines(procedure, [<<"--+ end", _/binary>>|Lines], Buffer, Queries) ->
    %% end of procedure. Output a new query
    Query = merge_lines_into_query( lists:reverse(Buffer) ),
    read_queries_from_lines(normal, Lines, [], [Query|Queries]);

read_queries_from_lines(procedure, [Line|Lines], Buffer, Queries) ->
    %% Add to buffer and continue processing
    read_queries_from_lines(procedure, Lines, [Line|Buffer],  Queries);

read_queries_from_lines(State, [<<"--", _/binary>>|Lines], Buffer, Queries) ->
    %% Comment line. Ignore in both normal and procedure states
    read_queries_from_lines(State, Lines, Buffer,  Queries);

read_queries_from_lines(State, [<<>>|Lines], Buffer, Queries) ->
    %% Ignore empty lines
    read_queries_from_lines(State, Lines, Buffer,  Queries);

read_queries_from_lines(normal, [Line|Lines], Buffer, Queries) ->
    %% Check if Line ends with a ;
    case is_end_of_query(Line) of
        true ->
            TrimLine = string:trim(Line, trailing, ";"),
            Query = merge_lines_into_query( lists:reverse( [TrimLine|Buffer] ) ),
            read_queries_from_lines(normal, Lines, [], [Query|Queries]);
        _ ->
            %% line is part of a query. add to buffer and continue
            read_queries_from_lines(normal, Lines, [Line|Buffer],  Queries)
    end;

read_queries_from_lines(_State, [], [], Queries) ->
    %% end of processing. return queries
    MQ = lists:reverse([ iolist_to_binary(Q) || Q <- Queries ]),
    lists:zip( lists:seq(1,length(MQ)), MQ);

read_queries_from_lines(_State, [], Buffer, _Queries) ->
    %% Nested procedure definition.
    Query = merge_lines_into_query( lists:reverse(Buffer) ),
    io:format("error~nIncomplete sql/procedure?~n...~s~n",[Query]),
    erlang:halt(1).

is_end_of_query(Line) ->
    case re:run(Line, ".*;$") of
        nomatch ->
            false;
        _ ->
            true
    end.

merge_lines_into_query(Lines) ->
    lists:join("  ", Lines).

execute_queries([], _) ->
    ignore;

execute_queries(Queries, Db) ->
    lists:foreach( fun({Id, Q}) ->
                           io:format("  query ~p....",[Id]),
                           case epgsql:equery(Db, Q) of
                               Res when element(1, Res) =:= ok ->
                                   io:format("ok~n");
                               {error, #error{code = Code}} ->
                                   io:format("~p~n",[epgsql_error_codename(Code)])
                           end
                   end, Queries),
    ok.

file_into_list( File ) ->
        {ok, IO} = file:open( File, [read] ),
        file_into_list( io:get_line(IO, ''), IO, [] ).


file_into_list( eof, _IO, Acc ) -> lists:reverse( Acc );
file_into_list( {error, _Error}, _IO, Acc ) -> lists:reverse( Acc );
file_into_list( Line, IO, Acc ) -> file_into_list( io:get_line(IO, ''), IO, [Line | Acc] ).

epgsql_error_codename(<<"0100C">>) -> dynamic_result_sets_returned;
epgsql_error_codename(<<"01008">>) -> implicit_zero_bit_padding;
epgsql_error_codename(<<"01003">>) -> null_value_eliminated_in_set_function;
epgsql_error_codename(<<"01007">>) -> privilege_not_granted;
epgsql_error_codename(<<"01006">>) -> privilege_not_revoked;
epgsql_error_codename(<<"01004">>) -> string_data_right_truncation;
epgsql_error_codename(<<"01P01">>) -> deprecated_feature;
epgsql_error_codename(<<"02000">>) -> no_data;
epgsql_error_codename(<<"02001">>) -> no_additional_dynamic_result_sets_returned;
epgsql_error_codename(<<"03000">>) -> sql_statement_not_yet_complete;
epgsql_error_codename(<<"08000">>) -> connection_exception;
epgsql_error_codename(<<"08003">>) -> connection_does_not_exist;
epgsql_error_codename(<<"08006">>) -> connection_failure;
epgsql_error_codename(<<"08001">>) -> sqlclient_unable_to_establish_sqlconnection;
epgsql_error_codename(<<"08004">>) -> sqlserver_rejected_establishment_of_sqlconnection;
epgsql_error_codename(<<"08007">>) -> transaction_resolution_unknown;
epgsql_error_codename(<<"08P01">>) -> protocol_violation;
epgsql_error_codename(<<"09000">>) -> triggered_action_exception;
epgsql_error_codename(<<"0A000">>) -> feature_not_supported;
epgsql_error_codename(<<"0B000">>) -> invalid_transaction_initiation;
epgsql_error_codename(<<"0F000">>) -> locator_exception;
epgsql_error_codename(<<"0F001">>) -> invalid_locator_specification;
epgsql_error_codename(<<"0L000">>) -> invalid_grantor;
epgsql_error_codename(<<"0LP01">>) -> invalid_grant_operation;
epgsql_error_codename(<<"0P000">>) -> invalid_role_specification;
epgsql_error_codename(<<"21000">>) -> cardinality_violation;
epgsql_error_codename(<<"22000">>) -> data_exception;
epgsql_error_codename(<<"2202E">>) -> array_subscript_error;
epgsql_error_codename(<<"22021">>) -> character_not_in_repertoire;
epgsql_error_codename(<<"22008">>) -> datetime_field_overflow;
epgsql_error_codename(<<"22012">>) -> division_by_zero;
epgsql_error_codename(<<"22005">>) -> error_in_assignment;
epgsql_error_codename(<<"2200B">>) -> escape_character_conflict;
epgsql_error_codename(<<"22022">>) -> indicator_overflow;
epgsql_error_codename(<<"22015">>) -> interval_field_overflow;
epgsql_error_codename(<<"2201E">>) -> invalid_argument_for_logarithm;
epgsql_error_codename(<<"2201F">>) -> invalid_argument_for_power_function;
epgsql_error_codename(<<"2201G">>) -> invalid_argument_for_width_bucket_function;
epgsql_error_codename(<<"22018">>) -> invalid_character_value_for_cast;
epgsql_error_codename(<<"22007">>) -> invalid_datetime_format;
epgsql_error_codename(<<"22019">>) -> invalid_escape_character;
epgsql_error_codename(<<"2200D">>) -> invalid_escape_octet;
epgsql_error_codename(<<"22025">>) -> invalid_escape_sequence;
epgsql_error_codename(<<"22P06">>) -> nonstandard_use_of_escape_character;
epgsql_error_codename(<<"22010">>) -> invalid_indicator_parameter_value;
epgsql_error_codename(<<"22020">>) -> invalid_limit_value;
epgsql_error_codename(<<"22023">>) -> invalid_parameter_value;
epgsql_error_codename(<<"2201B">>) -> invalid_regular_expression;
epgsql_error_codename(<<"22009">>) -> invalid_time_zone_displacement_value;
epgsql_error_codename(<<"2200C">>) -> invalid_use_of_escape_character;
epgsql_error_codename(<<"2200G">>) -> most_specific_type_mismatch;
epgsql_error_codename(<<"22004">>) -> null_value_not_allowed;
epgsql_error_codename(<<"22002">>) -> null_value_no_indicator_parameter;
epgsql_error_codename(<<"22003">>) -> numeric_value_out_of_range;
epgsql_error_codename(<<"22026">>) -> string_data_length_mismatch;
epgsql_error_codename(<<"22001">>) -> string_data_right_truncation;
epgsql_error_codename(<<"22011">>) -> substring_error;
epgsql_error_codename(<<"22027">>) -> trim_error;
epgsql_error_codename(<<"22024">>) -> unterminated_c_string;
epgsql_error_codename(<<"2200F">>) -> zero_length_character_string;
epgsql_error_codename(<<"22P01">>) -> floating_point_exception;
epgsql_error_codename(<<"22P02">>) -> invalid_text_representation;
epgsql_error_codename(<<"22P03">>) -> invalid_binary_representation;
epgsql_error_codename(<<"22P04">>) -> bad_copy_file_format;
epgsql_error_codename(<<"22P05">>) -> untranslatable_character;
epgsql_error_codename(<<"23000">>) -> integrity_constraint_violation;
epgsql_error_codename(<<"23001">>) -> restrict_violation;
epgsql_error_codename(<<"23502">>) -> not_null_violation;
epgsql_error_codename(<<"23503">>) -> foreign_key_violation;
epgsql_error_codename(<<"23505">>) -> unique_violation;
epgsql_error_codename(<<"23514">>) -> check_violation;
epgsql_error_codename(<<"24000">>) -> invalid_cursor_state;
epgsql_error_codename(<<"25000">>) -> invalid_transaction_state;
epgsql_error_codename(<<"25001">>) -> active_sql_transaction;
epgsql_error_codename(<<"25002">>) -> branch_transaction_already_active;
epgsql_error_codename(<<"25008">>) -> held_cursor_requires_same_isolation_level;
epgsql_error_codename(<<"25003">>) -> inappropriate_access_mode_for_branch_transaction;
epgsql_error_codename(<<"25004">>) -> inappropriate_isolation_level_for_branch_transaction;
epgsql_error_codename(<<"25005">>) -> no_active_sql_transaction_for_branch_transaction;
epgsql_error_codename(<<"25006">>) -> read_only_sql_transaction;
epgsql_error_codename(<<"25007">>) -> schema_and_data_statement_mixing_not_supported;
epgsql_error_codename(<<"25P01">>) -> no_active_sql_transaction;
epgsql_error_codename(<<"25P02">>) -> in_failed_sql_transaction;
epgsql_error_codename(<<"26000">>) -> invalid_sql_statement_name;
epgsql_error_codename(<<"27000">>) -> triggered_data_change_violation;
epgsql_error_codename(<<"28000">>) -> invalid_authorization_specification;
epgsql_error_codename(<<"2B000">>) -> dependent_privilege_descriptors_still_exist;
epgsql_error_codename(<<"2BP01">>) -> dependent_objects_still_exist;
epgsql_error_codename(<<"2D000">>) -> invalid_transaction_termination;
epgsql_error_codename(<<"2F000">>) -> sql_routine_exception;
epgsql_error_codename(<<"2F005">>) -> function_executed_no_return_statement;
epgsql_error_codename(<<"2F002">>) -> modifying_sql_data_not_permitted;
epgsql_error_codename(<<"2F003">>) -> prohibited_sql_statement_attempted;
epgsql_error_codename(<<"2F004">>) -> reading_sql_data_not_permitted;
epgsql_error_codename(<<"34000">>) -> invalid_cursor_name;
epgsql_error_codename(<<"38000">>) -> external_routine_exception;
epgsql_error_codename(<<"38001">>) -> containing_sql_not_permitted;
epgsql_error_codename(<<"38002">>) -> modifying_sql_data_not_permitted;
epgsql_error_codename(<<"38003">>) -> prohibited_sql_statement_attempted;
epgsql_error_codename(<<"38004">>) -> reading_sql_data_not_permitted;
epgsql_error_codename(<<"39000">>) -> external_routine_invocation_exception;
epgsql_error_codename(<<"39001">>) -> invalid_sqlstate_returned;
epgsql_error_codename(<<"39004">>) -> null_value_not_allowed;
epgsql_error_codename(<<"39P01">>) -> trigger_protocol_violated;
epgsql_error_codename(<<"39P02">>) -> srf_protocol_violated;
epgsql_error_codename(<<"3B000">>) -> savepoint_exception;
epgsql_error_codename(<<"3B001">>) -> invalid_savepoint_specification;
epgsql_error_codename(<<"3D000">>) -> invalid_catalog_name;
epgsql_error_codename(<<"3F000">>) -> invalid_schema_name;
epgsql_error_codename(<<"40000">>) -> transaction_rollback;
epgsql_error_codename(<<"40002">>) -> transaction_integrity_constraint_violation;
epgsql_error_codename(<<"40001">>) -> serialization_failure;
epgsql_error_codename(<<"40003">>) -> statement_completion_unknown;
epgsql_error_codename(<<"40P01">>) -> deadlock_detected;
epgsql_error_codename(<<"42000">>) -> syntax_error_or_access_rule_violation;
epgsql_error_codename(<<"42601">>) -> syntax_error;
epgsql_error_codename(<<"42501">>) -> insufficient_privilege;
epgsql_error_codename(<<"42846">>) -> cannot_coerce;
epgsql_error_codename(<<"42803">>) -> grouping_error;
epgsql_error_codename(<<"42830">>) -> invalid_foreign_key;
epgsql_error_codename(<<"42602">>) -> invalid_name;
epgsql_error_codename(<<"42622">>) -> name_too_long;
epgsql_error_codename(<<"42939">>) -> reserved_name;
epgsql_error_codename(<<"42804">>) -> datatype_mismatch;
epgsql_error_codename(<<"42809">>) -> wrong_object_type;
epgsql_error_codename(<<"42703">>) -> undefined_column;
epgsql_error_codename(<<"42883">>) -> undefined_function;
epgsql_error_codename(<<"42P01">>) -> undefined_table;
epgsql_error_codename(<<"42P02">>) -> undefined_parameter;
epgsql_error_codename(<<"42704">>) -> undefined_object;
epgsql_error_codename(<<"42701">>) -> duplicate_column;
epgsql_error_codename(<<"42P03">>) -> duplicate_cursor;
epgsql_error_codename(<<"42P04">>) -> duplicate_database;
epgsql_error_codename(<<"42723">>) -> duplicate_function;
epgsql_error_codename(<<"42P05">>) -> duplicate_prepared_statement;
epgsql_error_codename(<<"42P06">>) -> duplicate_schema;
epgsql_error_codename(<<"42P07">>) -> duplicate_table;
epgsql_error_codename(<<"42712">>) -> duplicate_alias;
epgsql_error_codename(<<"42710">>) -> duplicate_object;
epgsql_error_codename(<<"42702">>) -> ambiguous_column;
epgsql_error_codename(<<"42725">>) -> ambiguous_function;
epgsql_error_codename(<<"42P08">>) -> ambiguous_parameter;
epgsql_error_codename(<<"42P09">>) -> ambiguous_alias;
epgsql_error_codename(<<"42P10">>) -> invalid_column_reference;
epgsql_error_codename(<<"42611">>) -> invalid_column_definition;
epgsql_error_codename(<<"42P11">>) -> invalid_cursor_definition;
epgsql_error_codename(<<"42P12">>) -> invalid_database_definition;
epgsql_error_codename(<<"42P13">>) -> invalid_function_definition;
epgsql_error_codename(<<"42P14">>) -> invalid_prepared_statement_definition;
epgsql_error_codename(<<"42P15">>) -> invalid_schema_definition;
epgsql_error_codename(<<"42P16">>) -> invalid_table_definition;
epgsql_error_codename(<<"42P17">>) -> invalid_object_definition;
epgsql_error_codename(<<"44000">>) -> with_check_option_violation;
epgsql_error_codename(<<"53000">>) -> insufficient_resources;
epgsql_error_codename(<<"53100">>) -> disk_full;
epgsql_error_codename(<<"53200">>) -> out_of_memory;
epgsql_error_codename(<<"53300">>) -> too_many_connections;
epgsql_error_codename(<<"54000">>) -> program_limit_exceeded;
epgsql_error_codename(<<"54001">>) -> statement_too_complex;
epgsql_error_codename(<<"54011">>) -> too_many_columns;
epgsql_error_codename(<<"54023">>) -> too_many_arguments;
epgsql_error_codename(<<"55000">>) -> object_not_in_prerequisite_state;
epgsql_error_codename(<<"55006">>) -> object_in_use;
epgsql_error_codename(<<"55P02">>) -> cant_change_runtime_param;
epgsql_error_codename(<<"55P03">>) -> lock_not_available;
epgsql_error_codename(<<"57000">>) -> operator_intervention;
epgsql_error_codename(<<"57014">>) -> query_canceled;
epgsql_error_codename(<<"57P01">>) -> admin_shutdown;
epgsql_error_codename(<<"57P02">>) -> crash_shutdown;
epgsql_error_codename(<<"57P03">>) -> cannot_connect_now;
epgsql_error_codename(<<"58030">>) -> io_error;
epgsql_error_codename(<<"58P01">>) -> undefined_file;
epgsql_error_codename(<<"58P02">>) -> duplicate_file;
epgsql_error_codename(<<"F0000">>) -> config_file_error;
epgsql_error_codename(<<"F0001">>) -> lock_file_exists;
epgsql_error_codename(<<"P0000">>) -> plpgsql_error;
epgsql_error_codename(<<"P0001">>) -> raise_exception;
epgsql_error_codename(<<"P0002">>) -> no_data_found;
epgsql_error_codename(<<"P0003">>) -> too_many_rows;
epgsql_error_codename(<<"XX000">>) -> internal_error;
epgsql_error_codename(<<"XX001">>) -> data_corrupted;
epgsql_error_codename(<<"XX002">>) -> index_corrupted.





%% run_query(Worker, Migration) ->
%%     case has_migration_ran(Migration) of
%%         true ->
%%             ok;
%%         false ->
%%             case pgsql:equery(Worker, Migration:forwards()) of
%%                 {error, E} ->
%%                     error(E);
%%                 %% There are multiple ok type responses :(
%%                 Res when element(1, Res) == ok ->
%%                     Insert = ,

%%                     ok
%%             end
%%     end.
