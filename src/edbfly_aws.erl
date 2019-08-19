-module(edbfly_aws).

-export([
         get_secret/2
]).

-define(UNDEFINED, undefined).
-define(SECRETS_MANAGER_SERVICE, "secretsmanager").
-define(SECRETS_MANAGER_HOST(R), ?SECRETS_MANAGER_SERVICE ++ "." ++ R ++ ".amazonaws.com").
-define(SECRETS_MANAGER_URL(H), "https://" ++ H).
-define(AWS_CONTENT_TYPE, "application/x-amz-json-1.1").
-define(AWS_ALGORITHM, "AWS4-HMAC-SHA256").

%%====================================================================
%% API functions
%%====================================================================
get_secret(#{iam_ec2_role := Role, region := R}, SecretId) ->
    Cred = get_ec2_role_credentials(Role),
    get_secret(Cred#{region => R}, SecretId);

get_secret(#{access_key_id := _, secret_access_key := _, region := Region} = AwsConfig, SecretId) ->
    Method = "POST",
    Host = ?SECRETS_MANAGER_HOST(Region),
    Url = ?SECRETS_MANAGER_URL(Host),
    AmzTarget = "secretsmanager.GetSecretValue",
    Headers = [{"Host", Host},
               {"Content-Type", ?AWS_CONTENT_TYPE},
               {"X-Amz-Target", AmzTarget}],
    Payload = binary_to_list(jsone:encode( #{ 'SecretId' => list_to_binary(SecretId) } )),

    SignedHeaders = sign_headers(AwsConfig, ?SECRETS_MANAGER_SERVICE, Method, "/", "", Headers, Payload),

    {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} =
        httpc:request(post, {Url, SignedHeaders, ?AWS_CONTENT_TYPE, Payload}, [], []),
    Secrets = jsone:decode(list_to_binary(Body)),
    SecretString = maps:get(<<"SecretString">>, Secrets, undefined),
    SecretBinary = maps:get(<<"SecretBinary">>, Secrets, undefined),
    #{secret_string => SecretString, secret_binary => SecretBinary}.


%%====================================================================
%% internal functions
%%====================================================================
sign_headers(AwsConfig, Service, Method, CanonicalUrl, CanonicalQs, Headers, Payload) ->
    %% Credentials
    #{access_key_id := AccessKeyId, secret_access_key := SecretAccessKey, region := Region} = AwsConfig,
    Token = maps:get(token, AwsConfig, ?UNDEFINED),

    %% Get time and date headers
    Now = calendar:universal_time(),
    AmzDate = ec_date:format("YmdTHisZ", Now),
    Date = qdate:format("Ymd", Now),
    DateHeaders = add_date_header(AmzDate, Headers),

    %% cannonical request hash, signed headers
    {SignedHeaders, CanonicalRequestHash} = generate_signature_params(Method, CanonicalUrl, CanonicalQs, DateHeaders, Payload),

    %% sign the request
    CredentialScope = credential_scope(Date, Region, Service),
    SigningKey = signing_key(SecretAccessKey, Date, Region, Service),
    StringToSign = string_to_sign(AmzDate, CredentialScope, CanonicalRequestHash),
    Signature = hexdigest(sign(SigningKey, StringToSign)),

    %% Add authorization header
    Authorization = authorization(AccessKeyId, CredentialScope, SignedHeaders,
                                  Signature),
    AuthorizedHeaders = add_authorization_header(Authorization, DateHeaders),
    add_token_header(Token, AuthorizedHeaders).

add_token_header(?UNDEFINED, Headers) ->
    Headers;
add_token_header(Token, Headers) ->
    [{"X-Amz-Security-Token", Token}|Headers].

add_authorization_header(Authorization, Headers) ->
    [{"Authorization", Authorization}|Headers].

authorization(AccessKeyId, CredentialScope, SignedHeaders, Signature) ->
    ?AWS_ALGORITHM ++ " " ++ "Credential=" ++ AccessKeyId ++ "/" ++ CredentialScope ++ ", "
        ++  "SignedHeaders=" ++ SignedHeaders ++ ", " ++ "Signature=" ++ Signature.

string_to_sign(AmzDate, CredentialScope, CanonicalRequestHash) ->
    List = [?AWS_ALGORITHM, AmzDate, CredentialScope, CanonicalRequestHash],
    lists:flatten(lists:join("\n", List)).

sign(Key, Msg) when is_binary(Key), is_list(Msg) ->
    sign(Key, list_to_binary(Msg));

sign(Key, Msg) when is_list(Key), is_binary(Msg) ->
    sign(list_to_binary(Key), Msg);

sign(Key, Msg) when is_list(Key), is_list(Msg) ->
    sign(list_to_binary(Key), list_to_binary(Msg));

sign(Key, Msg) when is_binary(Key), is_binary(Msg) ->
    crypto:hmac(sha256, Key, Msg).

signing_key(SecretAccessKey, Date, Region, Service) ->
    KDate = sign("AWS4" ++ SecretAccessKey, Date),
    KRegion = sign(KDate, Region),
    KService = sign(KRegion, Service),
    KSigning = sign(KService, "aws4_request"),
    KSigning.

credential_scope(Date, Region, Service) ->
    lists:flatten( lists:join("/", [Date, Region, Service, "aws4_request"]) ).

generate_signature_params(Method, CanonicalUrl, CanonicalQs, Headers, Payload) ->
    {CanonicalHeaders, SignedHeaders} = generate_signature_headers(Headers),
    PayloadHash = hash(Payload),
    CanonicalRequest = lists:flatten(
                         lists:join("\n", [Method, CanonicalUrl, CanonicalQs,
                                           CanonicalHeaders, SignedHeaders, PayloadHash])),
    CanonicalRequestHash = hash(CanonicalRequest),
    {SignedHeaders, CanonicalRequestHash}.

generate_signature_headers(Headers) ->
    Fn = fun({Name, Value}, {CAcc, SAcc}) ->
                 N = string:strip(string:to_lower(Name)),
                 V = string:strip(Value),
                 C = N ++ ":" ++ V ++ "\n",
                 {[C|CAcc], [N|SAcc]}
         end,
    {CH, SH} = lists:foldl(Fn, {[],[]}, Headers),
    CanonicalHeaders = lists:flatten(lists:sort(CH)),
    SignedHeaders = lists:flatten(lists:join(";", lists:sort(SH))),
    {CanonicalHeaders, SignedHeaders}.

add_date_header(Date, Headers) ->
    [{"X-Amz-Date", Date}|Headers].

get_ec2_role_credentials(Role) ->
    Url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/" ++ Role,
    Response = httpc:request(get, {Url, []}, [], []),
    parse_ec2_role_credentials_response(Response).

parse_ec2_role_credentials_response({ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}}) ->
    #{<<"AccessKeyId">> := AccessKey, <<"SecretAccessKey">> := SecretKey, <<"Token">> := Token} = jsone:decode(list_to_binary(Body)),
    #{access_key_id => binary_to_list(AccessKey),
      secret_access_key => binary_to_list(SecretKey),
      token => binary_to_list(Token)};

parse_ec2_role_credentials_response(_) ->
    #{access_key_id => ?UNDEFINED, secret_access_key => ?UNDEFINED, token => ?UNDEFINED}.

hexdigest(Data) when is_binary(Data) ->
    bin2hex(Data).

hash(Data) when is_list(Data) ->
    hash( list_to_binary(Data) );
hash(Data) when is_binary(Data) ->
    hexdigest( crypto:hash(sha256, Data) ).

bin2hex(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X]));
bin2hex(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X]));
bin2hex(<<X:192/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~48.16.0b", [X]));
bin2hex(<<X:256/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~64.16.0b", [X]));
bin2hex(<<X:512/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~128.16.0b", [X]));
bin2hex(Binary) when is_binary(Binary) ->
    [ hex(C) || <<C:4>> <= Binary].

hex(0) ->
    $0;
hex(1) ->
    $1;
hex(2) ->
    $2;
hex(3) ->
    $3;
hex(4) ->
    $4;
hex(5) ->
    $5;
hex(6) ->
    $6;
hex(7) ->
    $7;
hex(8) ->
    $8;
hex(9) ->
    $9;
hex(10) ->
    $a;
hex(11) ->
    $b;
hex(12) ->
    $c;
hex(13) ->
    $d;
hex(14) ->
    $e;
hex(15) ->
    $f.
