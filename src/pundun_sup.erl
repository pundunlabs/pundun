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
    PPBServerOptions = get_ppb_server_options(),
    ?debug("mochi_socket_server options for protocol buffers: ~p",
	[PPBServerOptions]),
    PundunProtocolBuffersServer =
	{pundun_pb_server,
	 {mochiweb_socket_server, start_link, [PPBServerOptions]},
	 permanent, 5000, worker, [mochiweb_socket_server]},
    PundunCLI =
	{pundun_cli,
	 {pundun_cli, start_link, []},
	 permanent, 5000, worker, [pundun_cli]},
    {ok, { SupFlags, [PundunProtocolBuffersServer,
		      PundunCLI]} }.

%% ===================================================================
%% Internal Functions
%% ===================================================================
-spec get_ppb_server_options() -> Options :: [{atom(), term()}].
get_ppb_server_options() ->
    Params= gb_conf:get_param("pundun.yaml", ppb_server_options),
    PropList = fix_params(Params),
    [{loop, {pundun_bp_session, init, [[{handler, pundun_pb_handler}]]}}
      | PropList].

-spec fix_params(Params :: [{string(), any()}]) ->
    Fixed :: [{atom(), any()}].
fix_params(Params) ->
    fix_params(Params, []).

-spec fix_params(Params :: [{string(), any()}],
		 Acc :: [{atom(), any()}]) ->
    Fixed :: [{atom(), any()}].
fix_params([{"ip", "any"} | Rest], Acc) ->
    fix_params(Rest, [{ip, any} | Acc]);
fix_params([{"ssl_opts", SslOpts} | Rest], Acc) ->
    fix_params(Rest, [{ssl_opts, fix_ssl_opts(SslOpts)} | Acc]);
fix_params([{P, V} | Rest], Acc) ->
    fix_params(Rest, [{list_to_atom(P), V} | Acc]);
fix_params([], Acc) ->
    lists:reverse(Acc).

-spec fix_ssl_opts(Opts :: [{string(), term()}]) ->
    [{atom(), term()}].
fix_ssl_opts(Opts) ->
    fix_ssl_opts(Opts, []).

-spec fix_ssl_opts(Opts :: [{string(), any()}],
		   Acc :: [{atom(), term()}]) ->
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
    [{packet, 4} | lists:reverse(Acc)].
