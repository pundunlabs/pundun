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

-export([start_link/0]).
-export([init/1, terminate/2,
	 handle_call/3, handle_cast/2,
	 handle_info/2, code_change/3]).

-export([welcome_msg/0,
	 routines/0,
	 ip/0,
	 port/0,
	 system_dir/0,
	 user_dir/0]).

-export([show_tables/0,
	 table_info/1,
	 cm/1,
	 logger/1]).

-export([table_info_expand/1,
	 cm_expand/1,
	 logger_expand/1]).

-include_lib("gb_log/include/gb_log.hrl").
-include_lib("gb_conf/include/gb_conf.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    process_flag(trap_exit, true),
    case gb_cli_server:start_link(?MODULE) of
	{ok, SshDaemonRef} ->
	    {ok, #{ssh_daemon_ref => SshDaemonRef}};
	{error, Reason} ->
	    {stop, Reason, #{}}
    end.

handle_call(_, _, State) -> {reply, ok, State}.
handle_cast(_, State) -> {noreply, State}.
handle_info(_, State) -> {noreply, State}.
code_change(_,State,_) -> {ok, State}.
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


%%%===================================================================
%%% Routine Callbacks
%%% routine_callback() -> {ok, string()} | {stop, string()}.
%%% routine_callback(Args :: [string()]) ->
%%%    {ok, string()} | {stop, string()}.
%%%===================================================================
-spec show_tables() -> {ok, string()}.
show_tables() ->
    TableNames = gb_hash:all_entries(),
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
    TableNames = gb_hash:all_entries(),
    options_expand(Prefix, TableNames);
table_info_expand(["table_info", _Arg1 | Rest]) ->
    Attrs = get_table_info_attrs(),
    Prefix = lists:last(Rest),
    options_expand(Prefix, Attrs).

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
    S = lists:flatten(io_lib:format("\r~p: ~p", [P, V])),
    All = lists:reverse([S|Acc]),
    {ok, lists:concat(All)};
format_table_info([{P, V} | Rest], Acc) ->
    S = lists:flatten(io_lib:format("\r~p: ~p~n", [P, V])),
    format_table_info(Rest, [S|Acc]).

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
