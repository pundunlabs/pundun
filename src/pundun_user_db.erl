%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2015 Pundun Labs AB
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
%% @title
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(pundun_user_db).

-export([create_tables/1]).

-export([transaction/1]).

-include("pundun.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create tables on given Nodes.
%% @end
%%--------------------------------------------------------------------
-spec create_tables(Nodes :: [node()]) -> ok | {error, Reason :: any()}.
create_tables(Nodes) ->
    [create_table(Nodes, T) || T <- [pundun_user]].

%%--------------------------------------------------------------------
%% @doc
%% Run mnesia activity with access context transaction with given fun
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: fun()) ->
    {aborted, Reason :: term()} | {atomic, ResultOfFun :: term()}.
transaction(Fun) ->
    case catch mnesia:activity(transaction, Fun) of
	{'EXIT', Reason} ->
	    {error, Reason};
	Result ->
	    {atomic, Result}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec create_table(Nodes::[node()], Name::atom()) -> ok | {error, Reason::term()}.
create_table(Nodes, Name) when Name == pundun_user ->
    TabDef = [{access_mode, read_write},
	      {attributes, record_info(fields, pundun_user)},
	      {disc_copies, Nodes},
	      {load_order, 39},
	      {record_name, Name},
	      {type, set}
	     ],
    mnesia:create_table(Name, TabDef),
    ok = write_admin_user();
create_table(_, _) ->
    {error, "Unknown table definition"}.

-spec write_admin_user() -> ok.
write_admin_user() ->
    Salt = [crypto:rand_uniform(48,125) || _ <- lists:seq(0,15)],
    Normalized = stringprep:prepare("admin"),
    IterCount = 1024,
    SaltedPassword = scramerl_lib:hi(Normalized, Salt, IterCount),
    Record = #pundun_user{username = "admin",
			  salt = Salt,
			  iteration_count = IterCount,
			  salted_password = SaltedPassword},
    Fun = fun() -> mnesia:write(Record) end,
    {atomic, ok} = transaction(Fun),
    ok.
