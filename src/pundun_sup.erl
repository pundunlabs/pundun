-module(pundun_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("gb_log/include/gb_log.hrl").

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 4,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    PBPServerOptions = get_pbp_server_options(),
    ?debug("mochi_socket_server options for binary protocol (asn1): ~p",
	[PBPServerOptions]),
    PPBServerOptions = get_ppb_server_options(),
    ?debug("mochi_socket_server options for protocol buffers: ~p",
	[PPBServerOptions]),
    PundunBinaryProtocolServer =
	{pundun_bp_server,
	 {mochiweb_socket_server, start_link, [PBPServerOptions]},
	 permanent, 5000, worker, [mochiweb_socket_server]},
    PundunProtocolBuffersServer =
	{pundun_pb_server,
	 {mochiweb_socket_server, start_link, [PPBServerOptions]},
	 permanent, 5000, worker, [mochiweb_socket_server]},
    PundunCLI =
	{pundun_cli,
	 {pundun_cli, start_link, []},
	 permanent, 5000, worker, [pundun_cli]},
    {ok, { SupFlags, [PundunBinaryProtocolServer,
		      PundunProtocolBuffersServer,
		      PundunCLI]} }.

%% ===================================================================
%% Internal Functions
%% ===================================================================
-spec get_pbp_server_options() -> Options :: [{atom(), term()}].
get_pbp_server_options() ->
    Params= gb_conf:get_param("pundun.yaml", pbp_server_options),
    PropList = [{list_to_atom(P),V} || {P,V} <- Params],
    PropList1 = fix_ssl_opts(PropList),
    [{loop, {pundun_bp_session, init, [[{handler, pundun_bp_handler}]]}}
      | PropList1].

-spec get_ppb_server_options() -> Options :: [{atom(), term()}].
get_ppb_server_options() ->
    Params= gb_conf:get_param("pundun.yaml", ppb_server_options),
    PropList = [{list_to_atom(P),V} || {P,V} <- Params],
    PropList1 = fix_ssl_opts(PropList),
    [{loop, {pundun_bp_session, init, [[{handler, pundun_pb_handler}]]}}
      | PropList1].

-spec fix_ssl_opts(List :: [{atom(), term()}]) ->
    [{atom(), term()}].
fix_ssl_opts(List) ->
    SSLopts = proplists:get_value(ssl_opts, List),
    SSLopts1 = fix_ssl_opts(SSLopts, []),
    [{ssl_opts, SSLopts1} | proplists:delete(ssl_opts, List)].

-spec fix_ssl_opts(List :: [{atom(), term()}], Acc :: [{atom(), term()}]) ->
    [{atom(), term()}].
fix_ssl_opts([{"certfile", CertFile} | Rest], Acc) ->
    CertFilePath = filename:join(code:priv_dir(pundun), CertFile),
    fix_ssl_opts(Rest, [{certfile, CertFilePath} | Acc]);
fix_ssl_opts([{"keyfile", KeyFile} | Rest], Acc) ->
    KeyFilePath = filename:join(code:priv_dir(pundun), KeyFile),
    fix_ssl_opts(Rest, [{keyfile, KeyFilePath} | Acc]);
fix_ssl_opts([{K, V} | Rest], Acc) ->
    fix_ssl_opts(Rest, [{list_to_atom(K), V} | Acc]);
fix_ssl_opts([], Acc) ->
    Acc.
