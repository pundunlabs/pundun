%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2016 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(pundun_cli).

-behaviour(gen_server).

-export([start_link/0,
	 get_topology_commit_ids/0,
	 get_hash/1,
	 get_node_names/0,
	 get_node/1]).

-export([init/1, terminate/2,
	 handle_call/3, handle_cast/2,
	 handle_info/2, code_change/3]).

-export([welcome_msg/0,
	 routines/0,
	 ip/0,
	 port/0,
	 system_dir/0,
	 user_dir/0,
	 get_opts/0]).

-export([show_tables/0,
	 table_info/1,
	 cluster/1,
	 user/1,
	 cm/1,
	 logger/1]).

-export([table_info_expand/1,
	 cluster_expand/1,
	 user_expand/1,
	 cm_expand/1,
	 logger_expand/1]).

-include_lib("gb_log/include/gb_log.hrl").
-include_lib("gb_conf/include/gb_conf.hrl").

-spec get_topology_commit_ids() ->
    {ok, Ids :: [string()]} | {error, Reason :: term()}.
get_topology_commit_ids() ->
    gen_server:call(?MODULE, get_topology_commit_ids).

-spec get_hash(CommitId :: string()) ->
    {ok, Hash :: integer()} | {error, Reason :: term()}.
get_hash(CommitId) ->
    gen_server:call(?MODULE, {get_hash, CommitId}).

-spec get_node_names() ->
    {ok, NodeNames :: [string()]} | {error, Reason :: term()}.
get_node_names() ->
    gen_server:call(?MODULE, get_node_names).

-spec get_node(NodeName :: string()) ->
    {ok, Node :: node()} | {error, Reason :: term()}.
get_node(NodeName) ->
    gen_server:call(?MODULE, {get_node, NodeName}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    process_flag(trap_exit, true),
    case gb_cli_server:start_link(?MODULE) of
	{ok, SshDaemonRef} ->
	    discover_nodes(),
	    {ok, #{ssh_daemon_ref => SshDaemonRef}};
	{error, Reason} ->
	    {stop, Reason, #{}}
    end.

handle_call(get_topology_commit_ids, _, Map) ->
    {ok, Hist} = gb_dyno_metadata:fetch_topo_history(),
    HashList = [ Hash || {_,[{_, Hash}]} <- Hist],
    Ids = [lists:sublist(integer_to_list(H,16),6) || H <- HashList],
    Mappings = maps:from_list(lists:zip(Ids, HashList)),
    {reply, {ok, Ids}, maps:put(hash_mappings, Mappings, Map)};
handle_call({get_hash, Id}, _, Map = #{hash_mappings := Mappings}) ->
    Hash = maps:get(Id, Mappings, undefined),
    {reply, {ok, Hash}, Map};
handle_call(get_node_names, _, Map = #{nodes := Nodes} ) ->
    Names = [atom_to_list(N) || N <- Nodes],
    Mappings = maps:from_list(lists:zip(Names, Nodes)),
    {reply, {ok, Names}, maps:put(node_mappings, Mappings, Map)};
handle_call({get_node, Name}, _, Map = #{node_mappings := Mappings}) ->
    Node = maps:get(Name, Mappings, undefined),
    {reply, {ok, Node}, Map};
handle_call(_, _, Map) ->
    {reply, ok, Map}.

handle_cast({register_nodes, Nodes}, Map) ->
    Names = [atom_to_list(N) || N <- Nodes],
    Mappings = maps:from_list(lists:zip(Names, Nodes)),
    {noreply, Map#{nodes => Nodes, node_mappings => Mappings}};
handle_cast(_, M) ->
    {noreply, M}.

handle_info(_, M) -> {noreply, M}.

code_change(_,M,_) -> {ok, M}.

terminate(Reason, #{ssh_daemon_ref := SshDaemonRef}) ->
    ?debug("Terminating Pundun CLI, ~p", [Reason]),
    ssh:stop_daemon(SshDaemonRef).

%%%===================================================================
%%% CLI callbacks
%%%===================================================================
-spec welcome_msg() -> string().
welcome_msg() ->
    Params = gb_conf:get_param("pundun.yaml", pundun_cli_options),
    proplists:get_value("welcome_msg", Params,
	"Welcome to Pundun Command Line Interface").

-spec routines() -> map().
routines() ->
    #{"show_tables" => #{mfa => {?MODULE, show_tables, 0},
			 usage => "show_tables",
			 desc => "Show all existing database tables."},
      "table_info" => #{mfa => {?MODULE, table_info, 1},
			expand => {?MODULE, table_info_expand, 1},
			usage => table_info_usage(),
			desc => "Print table information on given attributes."},
      "cluster" => #{mfa => {?MODULE, cluster, 1},
		     expand => {?MODULE, cluster_expand, 1},
		     usage => cluster_usage(),
		     desc => "Cluster management command. See usage."},
      "user" => #{mfa => {?MODULE, user, 1},
		  expand => {?MODULE, user_expand, 1},
		  usage => user_usage(),
		  desc => "User management command. See usage."},
      "cm" => #{mfa => {?MODULE, cm, 1},
		expand => {?MODULE, cm_expand, 1},
		usage => cm_usage(),
		desc => "Configuration management command. See usage."},
      "logger" => #{mfa => {?MODULE, logger, 1},
		    expand => {?MODULE, logger_expand, 1},
		    usage => logger_usage(),
		    desc => "Log management command. See usage."}}.

-spec ip() -> string() | any.
ip() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    proplists:get_value("ip", Params, any).

-spec port() -> integer().
port() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    proplists:get_value("port", Params, 8989).

-spec system_dir() -> string().
system_dir() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    Dir = proplists:get_value("system_dir", Params, "ssh"),
    filename:join(code:priv_dir(pundun), Dir).

-spec user_dir() -> string().
user_dir() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    Dir = proplists:get_value("user_dir", Params, "ssh"),
    filename:join(code:priv_dir(pundun), Dir).

-spec get_opts() -> [term()].
get_opts() ->
    [{pwdfun, fun pundun_user_db:verify_user/2}].


%%%===================================================================
%%% Routine Callbacks
%%% routine_callback() -> {ok, string()} | {stop, string()}.
%%% routine_callback(Args :: [string()]) ->
%%%    {ok, string()} | {stop, string()}.
%%%===================================================================
-spec show_tables() -> {ok, string()}.
show_tables() ->
    TableNames = enterdb:list_tables(),
    Spaced = [ T ++" " || T <- TableNames],
    {ok, lists:concat(Spaced)}.

-spec table_info([string()]) -> {ok, string()}.
table_info([TabName]) ->
    case enterdb:table_info(TabName) of
	{ok, List} ->
	    format_table_info(List);
	{error, Reason} ->
	    {ok, Reason}
    end;
table_info([TabName | Attrs]) ->
    Params = attr_atoms(Attrs),
    case enterdb:table_info(TabName, Params) of
	{ok, List} ->
	    format_table_info(List);
	{error, Reason} ->
	    {ok, Reason}
    end.

-spec cluster([string()]) -> {ok, string()}.
cluster(["show"]) ->
    {ok, Topo} = gb_dyno_metadata:lookup_topo(),
    {ok, print_topo(Topo)};
cluster(["show", CommitID]) ->
    {ok, Hash} = get_hash(CommitID),
    {ok, Topo} = gb_dyno_metadata:lookup_topo(Hash),
    {ok, print_topo(Topo)};
cluster(["pull", NodeStr]) ->
    {ok, Node} = get_node(NodeStr),
    Result = gb_dyno_gossip:pull(Node),
    {ok, print(Result)};
cluster(["add_host", Host]) ->
    Result = add_host(Host),
    {ok, print(Result)};
cluster(_) ->
    {ok, "Unrecognised cluster option."}.

-spec user([string()]) -> {ok, string()}.
user(["add", User, Passwd]) ->
    case pundun_user_db:add_user(User, Passwd) of
	{ok, U} ->
	    {ok, print(U)};
	{error, R} ->
	    {ok, print(R)}
    end;
user(["delete", User]) ->
    Result = pundun_user_db:del_user(User),
    {ok, print(Result)};
user(["passwd", User, Passwd]) ->
    Result = pundun_user_db:passwd(User, Passwd),
    {ok, print(Result)};
user(_) ->
    {ok, "Unrecognised user option."}.

-spec cm([string()]) -> {ok, string()}.
cm(["list"]) ->
    Cfgs = gb_conf:list(),
    {ok, add_space(Cfgs)};
cm(["show", Cfg]) ->
    AppConf = gb_conf:show(Cfg),
    {ok, pp_gb_conf_appconf(AppConf)};
cm(["show", Cfg, Version]) ->
    case string:to_integer(Version) of
	{error, _} ->
	    {ok, "Invalid version number."};
	{V, _} ->
	    AppConf = gb_conf:show(Cfg, V),
	    {ok, pp_gb_conf_appconf(AppConf)}
    end;
cm(["versions", Cfg]) ->
    Versions = gb_conf:versions(Cfg),
    {ok, print_cm_versions(Versions)};
cm(["load", Cfg]) ->
    case gb_conf:load(Cfg) of
	{ok, V} ->
	    {ok, "Version " ++ print(V) ++ " loaded."};
	{error, E} ->
	    {ok, lists:flatten(io_lib:format("error: ~p", [E]))}
    end;
cm(["activate", Cfg]) ->
    case gb_conf:activate(Cfg) of
	ok ->
	    {ok, "Latest version activated."};
	{error, E} ->
	    {ok, lists:flatten(io_lib:format("error: ~p", [E]))}
    end;
cm(["activate", Cfg, Version]) ->
    V = erlang:list_to_integer(Version),
    case gb_conf:activate(Cfg, V) of
	ok ->
	    {ok, "Version "++print(Version)++" activated."};
	{error, E} ->
	    {ok, lists:flatten(io_lib:format("error: ~p", [E]))}
    end;
cm(["tag", Cfg, Version, Tag]) ->
    V = erlang:list_to_integer(Version),
    case gb_conf:tag(Cfg, V, Tag) of
	ok ->
	    {ok, "Version "++print(Version)++" tagged."};
	{error, E} ->
	    {ok, lists:flatten(io_lib:format("error: ~p", [E]))}
    end;
cm(["get_param", Cfg, Param]) ->
    V = gb_conf:get_param(Cfg, Param),
    {ok, Param ++": "++ print_value(V, "\r\n", "")};
cm(_) ->
    {ok, "Unrecognised cm option."}.

-spec logger([string()]) -> {ok, string()}.
logger(["current_filter"]) ->
    Name = gb_log_filter:name(),
    Level = gb_log_filter:level(),
    {ok, "Name\tLevel\n\r" ++ erlang:atom_to_list(Name) ++
	 "\t" ++  erlang:atom_to_list(Level)};
logger(["available_filters"]) ->
    Filters = [erlang:atom_to_list(F) || F <- gb_log_oam:available_filters()],
    {ok, add_space(Filters)};
logger(["set_filter", Filter]) ->
    Result =
	case check_log_filter_name(Filter) of
	    {ok, F} ->
		(catch gb_log_oam:load_filter(F));
	    {error, Reason} ->
		{error, Reason}
	end,
    case Result of
	{module, _} ->
	    {ok, "Current filter set to "  ++ Filter};
	_ ->
	    {ok, "Failed to set filter to " ++ Filter}
    end;
logger(["generate_filters"]) ->
    case (catch gb_log_oam:load_store_filters_beam()) of
	ok ->
	    {ok, "Filters generated from filters.cfg file."};
	_ ->
	    {ok, "Failed to generate filters"}
    end;
logger(Else) ->
    ?debug("Unrecognised logger option: ~p",[Else]),
    {ok, "Unrecognised logger option."}.

%%%===================================================================
%%% Expand Callbacks
%%% expand_callback(Args :: [string()]) -> {ok, [string()]}.
%%%===================================================================
-spec table_info_expand([string()]) -> {ok, [string()]}.
table_info_expand(["table_info", Prefix]) ->
    TableNames = enterdb:list_tables(),
    options_expand(Prefix, TableNames);
table_info_expand(["table_info", _Arg1 | Rest]) ->
    Attrs = get_table_info_attrs(),
    Prefix = lists:last(Rest),
    options_expand(Prefix, Attrs).

-spec cluster_expand([string()]) -> {ok, string()}.
cluster_expand(["cluster", Prefix])->
    Options = ["add_host", "pull", "show"],
    options_expand(Prefix, Options);
cluster_expand(["cluster", "show", Prefix])->
    {ok, Options} = get_topology_commit_ids(),
    options_expand(Prefix, Options);
cluster_expand(["cluster", "pull", Prefix])->
    {ok, Options} = get_node_names(),
    options_expand(Prefix, Options);
cluster_expand(_) ->
    {ok, []}.

-spec user_expand([string()]) -> {ok, string()}.
user_expand(["user", Prefix])->
    Options = ["add", "delete", "passwd"],
    options_expand(Prefix, Options);
user_expand(["user", "delete", Prefix])->
    Options = pundun_user_db:list_users(),
    options_expand(Prefix, Options);
user_expand(["user", "passwd", Prefix])->
    Options = pundun_user_db:list_users(),
    options_expand(Prefix, Options);
user_expand(_) ->
    {ok, []}.

-spec cm_expand([string()]) -> {ok, string()}.
cm_expand(["cm", Prefix])->
    Options = ["get_param", "tag", "activate", "load", "versions",
	       "show", "list"],
    options_expand(Prefix, Options);
cm_expand(["cm", Cmd, Prefix]) when Cmd == "show";
				    Cmd == "versions";
				    Cmd == "load";
				    Cmd == "activate";
				    Cmd == "tag";
				    Cmd == "get_param" ->
    Options = gb_conf:list(),
    options_expand(Prefix, Options);
cm_expand(["cm", Cmd, Conf, Prefix]) when Cmd == "show";
					  Cmd == "activate";
					  Cmd == "tag" ->
    Versions = gb_conf:versions(Conf),
    options_expand(Prefix, [print(V) || {V,_} <- Versions]);
cm_expand(_) ->
    {ok, []}.

-spec logger_expand([string()]) -> {ok, string()}.
logger_expand(["logger", Prefix])->
    Options = ["generate_filters", "set_filter",
	       "available_filters", "current_filter"],
    options_expand(Prefix, Options);
logger_expand(["logger", "set_filter", Prefix])->
    Options = [erlang:atom_to_list(F) || F <- gb_log_oam:available_filters()],
    options_expand(Prefix, Options);
logger_expand(_) ->
    {ok, []}.

-spec options_expand(Prefix :: string(), List :: [string()]) ->
    {ok, [string()]}.
options_expand(Prefix, List) ->
    Fun =
	fun(Elem, Acc) ->
	    case lists:prefix(Prefix, Elem) of
		true -> [Elem|Acc];
		_ -> Acc
	    end
	end,
    Options = lists:foldl(Fun, [], List),
    {ok, Options}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec check_log_filter_name(Name :: string()) ->
    {ok, F :: atom()} | {error, Reason :: term()}.
check_log_filter_name(Name) ->
    Filters = [erlang:atom_to_list(F) || F <- gb_log_oam:available_filters()],
    check_log_filter_name(Name, Filters).

-spec check_log_filter_name(Name :: string(), Filters :: [string()]) ->
    {ok, F :: atom()} | {error, Reason :: term()}.
check_log_filter_name(Name, [Name|_]) ->
    {ok, erlang:list_to_atom(Name)};
check_log_filter_name(Name, [_|Rest]) ->
    check_log_filter_name(Name, Rest);
check_log_filter_name(_Name, []) ->
    {error, "No such filter."}.

-spec format_table_info([term() | none]) -> {ok, string()}.
format_table_info(List) ->
    ?debug("List: ~p",[List]),
    format_table_info([L || L <- List, L =/= none], []).

-spec format_table_info([term()], Acc :: string()) ->
    {ok, string()}.
format_table_info([], []) ->
    {ok, "no available info"};
format_table_info([{P, V}], Acc) ->
    S = table_attr_to_str(P, V),
    All = lists:reverse([S | Acc]),
    {ok, re:replace(lists:concat(All), "\n", "&\r", [global, {return, list}])};
format_table_info([{P, V} | Rest], Acc) ->
    S = table_attr_to_str(P, V) ++ "\n",
    format_table_info(Rest, [S | Acc]).

table_attr_to_str(size, V) ->
    PrintV = format_byte_unit(V, 0),
    lists:flatten(io_lib:format("\rsize: ~s", [PrintV]));
table_attr_to_str(P, V) ->
    lists:flatten(io_lib:format("\r~p: ~p", [P, V])).

format_byte_unit(Size, 5) ->
    lists:flatten(io_lib:format("~.2f ~s", [Size, format_size_unit(5)]));
format_byte_unit(Size, Level) when Size < 1024 ->
    lists:flatten(io_lib:format("~.2f ~s", [Size, format_size_unit(Level)]));
format_byte_unit(Size, Level) ->
    format_byte_unit(Size/1024, Level+1).

format_size_unit(0) -> "B";
format_size_unit(1) -> "KB";
format_size_unit(2) -> "MB";
format_size_unit(3) -> "GB";
format_size_unit(4) -> "TB";
format_size_unit(5) -> "PB".

-spec attr_atoms(Attrs :: [string()]) -> [atom()].
attr_atoms(Attrs) ->
    attr_atoms(Attrs, []).

attr_atoms(["name"|Rest], Params) ->
    attr_atoms(Rest, [name|Params]);
attr_atoms(["path"|Rest], Params) ->
    attr_atoms(Rest, [path|Params]);
attr_atoms(["key"|Rest], Params) ->
    attr_atoms(Rest, [key|Params]);
attr_atoms(["columns"|Rest], Params) ->
    attr_atoms(Rest, [columns|Params]);
attr_atoms(["indexes"|Rest], Params) ->
    attr_atoms(Rest, [indexes|Params]);
attr_atoms(["comparator"|Rest], Params) ->
    attr_atoms(Rest, [comparator|Params]);
attr_atoms(["data_model"|Rest], Params) ->
    attr_atoms(Rest, [data_model|Params]);
attr_atoms(["shards"|Rest], Params) ->
    attr_atoms(Rest, [shards|Params]);
attr_atoms(["size"|Rest], Params) ->
    attr_atoms(Rest, [size|Params]);
attr_atoms(["type"|Rest], Params) ->
    attr_atoms(Rest, [type|Params]);
attr_atoms(["wrapper"|Rest], Params) ->
    attr_atoms(Rest, [wrapper|Params]);
attr_atoms(["time_series"|Rest], Params) ->
    attr_atoms(Rest, [time_series|Params]);
attr_atoms(["distributed"|Rest], Params) ->
    attr_atoms(Rest, [distributed|Params]);
attr_atoms(["replication_factor"|Rest], Params) ->
    attr_atoms(Rest, [replication_factor|Params]);
attr_atoms(["clusters"|Rest], Params) ->
    attr_atoms(Rest, [clusters|Params]);
attr_atoms([_|Rest], Params) ->
    attr_atoms(Rest, Params);
attr_atoms([], Params) ->
    lists:reverse(Params).

-spec table_info_usage() -> string().
table_info_usage() ->
    "table_info TABLE_NAME [ATTR]\n"++
    "\rATTR:\n"++
    print_table_info_attrs().

-spec print_table_info_attrs() -> string().
print_table_info_attrs() ->
    Attrs = get_table_info_attrs(),
    print_table_info_attrs(Attrs, []).

-spec cluster_usage() -> string().
cluster_usage() ->
    "cluster OPTION\n\rOPTION:\n"++
    "\r\tshow [COMMIT_ID]\n"++
    "\r\tpull NODE\n"++
    "\r\tadd_host HOSTNAME\n"++
    "\rCOMMIT_ID:\n\r\tinteger\n"++
    "\rNODE:\n\r\tremote node name\n"++
    "\rHOSTNAME:\n\r\tstring\n".

-spec user_usage() -> string().
user_usage() ->
    "user OPTION\n\rOPTION:\n"++
    "\r\tadd USER PASSWD\n"++
    "\r\tdelete USER\n"++
    "\r\tpasswd USER\n"++
    "\rUSER:\n\r\tstring\n"++
    "\rPASSWD:\n\r\tstring\n".

-spec cm_usage() -> string().
cm_usage() ->
    "cm OPTION\n\rOPTION:\n"++
    "\r\tlist\n"++
    "\r\tshow CFG [VERSION]\n"++
    "\r\tversions CFG\n"++
    "\r\tload CFG\n"++
    "\r\tactivate CFG [VERSION]\n"++
    "\r\ttag CFG VERSION TAG\n"++
    "\r\tget_param CFG PARAM\n"++
    "\rCFG:\n\r\tFilename\n"++
    "\rVERSION:\n\r\tinteger\n".

-spec logger_usage() -> string().
logger_usage() ->
    "logger OPTION\n\rOPTION:\n"++
    "\r\tcurrent_filter\n"++
    "\r\tavailable_filters\n"++
    "\r\tset_filter FILTER\n"++
    "\r\tgenerate_filters\n" ++
    "\rFILTER:\n\r\tstring\n".


-spec print_table_info_attrs([string()], string()) ->
    string().
print_table_info_attrs([A|Rest], Acc) ->
    print_table_info_attrs(Rest, "\r\t" ++ A ++ "\n"++ Acc);
print_table_info_attrs([], Acc) ->
    Acc.

-spec get_table_info_attrs() -> string().
get_table_info_attrs() ->
    ["clusters","replication_factor","distributed",
     "time_series","wrapper","type","size","data_model","shards",
     "comparator","indexes","columns","key","path","name"].

-spec add_space([L :: string()]) -> string().
add_space(L) ->
    add_space(L, []).

-spec add_space([string()], string()) -> string().
add_space([E|Rest], Acc) ->
    add_space(Rest, Acc ++ E ++ " ");
add_space([], Acc) ->
    Acc.

-spec pp_gb_conf_appconf(#gb_conf_appconf{} | undefined) ->
    string().
pp_gb_conf_appconf(undefined) ->
    "No such configuration.";
pp_gb_conf_appconf(#gb_conf_appconf{name = Name,
				    file = File,
				    version = Version,
				    active = Active,
				    tag = Tag,
				    conf =  Proplist}) ->
    "\n\rName: " ++ print(Name) ++
    "\n\rFile: " ++ print(File) ++
    "\n\rVersion: " ++ print(Version) ++
    "\n\rActive: " ++ print(Active) ++
    "\n\rTag: " ++ print(Tag) ++
    "\n\rConfiguration:" ++ print_pl(Proplist, "\n\r\t", "").

-spec print_pl([{term(), term()} | term()],
		Prefix :: string(),
		Acc :: string()) ->
    string().
print_pl([{P, V}|Rest], Prefix, Acc) ->
    NewAcc = Acc ++ Prefix ++ print(P) ++": " ++ print_value(V, Prefix, ""),
    print_pl(Rest, Prefix, NewAcc);
print_pl([V|Rest], Prefix, Acc) ->
    NewAcc = Acc ++ Prefix ++ print_value(V, Prefix, ""),
    print_pl(Rest, Prefix, NewAcc);
print_pl([], _Prefix, Acc) ->
    Acc.

-spec print_value(V :: term(), Prefix :: string(), Acc :: string()) ->
    string().
print_value(V, Prefix, Acc) ->
    case print(V) of
	nok ->
	    print_pl(V, Prefix ++ "\t", Acc);
	Else ->
	    Else
    end.

-spec print(integer() | atom() | [term()]) ->
    string() | nok.
print(undefined) ->
    "";
print(A) when is_integer(A) ->
    erlang:integer_to_list(A);
print(A) when is_atom(A) ->
    erlang:atom_to_list(A);
print(L) when is_list(L)->
    case io_lib:printable_list(L) of
	true ->
	    L;
	false ->
	    nok
    end.

-spec print_cm_versions(V :: [{integer(), boolean()}]) ->
    string().
print_cm_versions(V) ->
    print_cm_versions(V, "Version\tActive\n\r").

-spec print_cm_versions(V :: [{integer(), boolean()}],
			Acc :: string()) ->
    string().
print_cm_versions([{V, B} | Rest], Acc) ->
    Line = print(V) ++ "\t" ++ print(B) ++ "\n\r",
    print_cm_versions(Rest, Acc ++ Line);
print_cm_versions([], Acc) ->
    Acc.

-spec print_topo(Topo :: [{Attr :: atom(), Value :: term()}] ) ->
    string().
print_topo(Topo) ->
    CName = proplists:get_value(cluster, Topo),
    Nodes = print_nodes(proplists:get_value(nodes, Topo)),
    "Cluster: " ++ CName ++ "\n\r" ++ Nodes.

-spec print_nodes(Nodes :: [{Node :: atom(), Properties :: [term()]}]) ->
    string().
print_nodes(Nodes) ->
    "Node\t\t\tDC\tRack\tVersion\n\r" ++ print_nodes(Nodes, []).

print_nodes([{Node, Proplist} | Rest], Acc) ->
    DC = proplists:get_value(dc, Proplist),
    Rack = proplists:get_value(rack, Proplist),
    Version = proplists:get_value(version, Proplist),
    Str = print(Node) ++ "\t" ++
	  print(DC) ++ "\t" ++
	  print(Rack) ++ "\t" ++
	  print(Version) ++ "\n\r",
    print_nodes(Rest, [Str|Acc]);
print_nodes([], Acc) ->
    lists:flatten(lists:reverse(Acc)).

add_host(Host) ->
    Port = os:getenv("ERL_EPMD_PORT", 4369),
    case gen_tcp:connect(Host, Port, [], 200) of
	{ok, Socket} ->
	    gen_tcp:close(Socket),
	    add_host_(Host);
	{error, _} ->
	    "can not connect to host epmd."
    end.

add_host_(Host) ->
    discover_nodes([list_to_atom(Host)]),
    ok.

discover_nodes(Hosts) ->
    spawn(fun() ->
	gen_server:cast(?MODULE, {register_nodes, net_adm:world_list(Hosts)})
    end).
discover_nodes() ->
    spawn(fun() ->
	gen_server:cast(?MODULE, {register_nodes, nodes()})
    end).
