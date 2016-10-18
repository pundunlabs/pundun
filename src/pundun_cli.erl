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

-export([start_link/0]).

-export([welcome_msg/0,
	 routines/0,
	 ip/0,
	 port/0,
	 system_dir/0,
	 user_dir/0]).

-export([show_tables/0,
	 table_info/1,
	 help/0]).

-include("gb_log.hrl").

start_link() ->
    gb_cli_server:start_link(?MODULE).

%%%===================================================================
%%% CLI callbacks
%%%===================================================================

welcome_msg() ->
    Params = gb_conf:get_param("pundun.yaml", pundun_cli_options),
    proplists:get_value("welcome_msg", Params,
	"Welcome to Pundun Command Line Interface").

routines() ->
    #{"show_tables" => #{mfa => {?MODULE, show_tables, 0},
			 usage => "show_tables",
			 desc => "Show all existing database tables."},
      "table_info" => #{mfa => {?MODULE, table_info, 1},
			usage => table_info_usage(),
			desc => "Print table information on given attributes."},
      "help" => #{mfa => {?MODULE, help, 0},
		  usage => "help",
		  desc => "Print usage information."}}.

ip() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    proplists:get_value("ip", Params, any).

port() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    proplists:get_value("port", Params, 8989).

system_dir() ->
    Params= gb_conf:get_param("pundun.yaml", pundun_cli_options),
    Dir = proplists:get_value("system_dir", Params, "ssh"),
    filename:join(code:priv_dir(pundun), Dir).

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
show_tables() ->
    TableNames = gb_hash:all_entries(),
    Spaced = [ T ++" " || T <- TableNames],
    {ok, lists:concat(Spaced)}.

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

help()->
    {ok, "CMD [OPTIONS]"}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
format_table_info(List) ->
    ?debug("List: ~p",[List]),
    format_table_info([L || L <- List, L =/= none], []).

format_table_info([], []) ->
    {ok, "no available info"};
format_table_info([{P, V}], Acc) ->
    S = lists:flatten(io_lib:format("\r~p: ~p", [P, V])),
    All = lists:reverse([S|Acc]),
    {ok, lists:concat(All)};
format_table_info([{P, V} | Rest], Acc) ->
    S = lists:flatten(io_lib:format("\r~p: ~p~n", [P, V])),
    format_table_info(Rest, [S|Acc]).

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

table_info_usage() ->
    "table_info TABLE_NAME [ATTR]\n"++
    "\rATTR:\n"++
    "\r\tname\n"++
    "\r\tpath\n"++
    "\r\tkey\n"++
    "\r\tcolumns\n"++
    "\r\tindexes\n"++
    "\r\tcomparator\n"++
    "\r\tshards\n"++
    "\r\tdata_model\n"++
    "\r\tsize\n"++
    "\r\ttype\n"++
    "\r\twrapper\n"++
    "\r\ttime_series\n"++
    "\r\tdistributed\n"++
    "\r\treplication_factor\n"++
    "\r\tclusters\n".
