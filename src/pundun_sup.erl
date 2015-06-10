-module(pundun_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

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
    PundunBinaryProtocolServer =
	{mochiweb_socket_server,
	 {mochiweb_socket_server, start_link, [PBPServerOptions]},
	 permanent, 5000, worker, [mochiweb_socket_server]},
    {ok, { SupFlags, [PundunBinaryProtocolServer]} }.

%% ===================================================================
%% Internal Functions
%% ===================================================================
-spec get_pbp_server_options() -> Options :: [{atom(), term()}].
get_pbp_server_options() ->
    Params= gb_conf:get_param("pundun.yaml", pbp_server_options),
    PropList = [{list_to_atom(P),V} || {P,V} <- Params],
    [{loop, {pundun_bp_session, init}} | PropList].
    
