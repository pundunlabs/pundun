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

-export([create_tables/1,
	 add_user/2,
	 add_user/3,
	 del_user/1,
	 passwd/2,
	 upd_details/2,
	 details/2,
	 list_users/0,
	 verify_user/2]).

-export([transaction/1]).

-include("pundun.hrl").
-include_lib("gb_log/include/gb_log.hrl").

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

-spec add_user(User :: string(), PassWd :: string()) ->
    {ok, User :: string()} | {error, Reason :: term()}.
add_user(User, PassWd) ->
    case transaction(fun()-> add_user_fun(User, PassWd, #{}) end) of
	{atomic, {ok, U}} ->
	    {ok, U};
	{atomic, Else} ->
	    Else;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec add_user(User :: string(), PassWd :: string(), Details :: map()) ->
    {ok, User :: string()} | {error, Reason :: term()}.
add_user(User, PassWd, Details) ->
    case transaction(fun()-> add_user_fun(User, PassWd, Details) end) of
	{atomic, {ok, U}} ->
	    {ok, U};
	{atomic, Else} ->
	    Else;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec add_user_fun(User :: string(), PassWd :: string(), Details :: map()) ->
    {ok, User :: string()} | {error, Reason :: term()}.
add_user_fun(User, PassWd, Details) ->
    User_ = stringprep:prepare(User, saslprep),
    case mnesia:read(pundun_user, User_) of
	[] ->
	    PassWd_ = stringprep:prepare(PassWd, saslprep),
	    Salt = get_salt(16),
	    IterCount = 4096,
	    SaltedPassword = scramerl_lib:hi(PassWd_, Salt, IterCount),
	    Record = #pundun_user{username = User_,
				  salt = Salt,
				  iteration_count = IterCount,
				  salted_password = SaltedPassword,
				  user_details = Details},
	    ok = mnesia:write(Record),
	    {ok, User_};
	[_] ->
	    {error, user_exists};
	Else ->
	    Else
    end.

-spec del_user(User :: string()) ->
    ok | {error, Reason :: term()}.
del_user(User) ->
    User_ = stringprep:prepare(User, saslprep),
    case transaction(fun() -> mnesia:delete(pundun_user, User_, write) end) of
	{atomic, ok} -> ok;
	Else -> Else
    end.

-spec passwd(User :: string(), Passwd :: string()) ->
    ok | {error, Reason :: term()}.
passwd(User, PassWd) ->
    case transaction(fun()-> passwd_fun(User, PassWd) end) of
	{atomic, {ok, U}} ->
	    {ok, U};
	{atomic, Else} ->
	    Else;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec passwd_fun(User :: string(), PassWd :: string()) ->
    {ok, User :: string()} | {error, Reason :: term()}.
passwd_fun(User, PassWd) ->
    User_ = stringprep:prepare(User, saslprep),
    case mnesia:read(pundun_user, User_) of
	[R] ->
	    PassWd_ = stringprep:prepare(PassWd, saslprep),
	    Salt = get_salt(16),
	    IterCount = R#pundun_user.iteration_count,
	    SaltedPassword = scramerl_lib:hi(PassWd_, Salt, IterCount),
	    Record = R#pundun_user{salt = Salt,
				   salted_password = SaltedPassword},
	    mnesia:write(Record);
	[] ->
	    {error, user_not_exists};
	Else ->
	    Else
    end.

-spec upd_details(User :: string(), Details :: map()) ->
    ok | {error, Reason :: term()}.
upd_details(User, Details) ->
    case transaction(fun()-> upd_details_fun(User, Details) end) of
	{atomic, {ok, U}} ->
	    {ok, U};
	{atomic, Else} ->
	    Else;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec upd_details_fun(User :: string(), Details :: map()) ->
    {ok, User :: string()} | {error, Reason :: term()}.
upd_details_fun(User, Details) ->
    User_ = stringprep:prepare(User, saslprep),
    case mnesia:read(pundun_user, User_) of
	[R] ->
	    CurrDetails = R#pundun_user.user_details,
	    Record = R#pundun_user{
			    user_details = 
				maps:merge(CurrDetails, Details)
				   },
	    mnesia:write(Record);
	[] ->
	    {error, user_not_exists};
	Else ->
	    Else
    end.

-spec details(User :: string(), Details :: map()) ->
    ok | {error, Reason :: term()}.
details(User, Details) ->
    case transaction(fun()-> details_fun(User, Details) end) of
	{atomic, {ok, U}} ->
	    {ok, U};
	{atomic, Else} ->
	    Else;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec details_fun(User :: string(), Details :: map()) ->
    {ok, User :: string()} | {error, Reason :: term()}.
details_fun(User, Details) ->
    User_ = stringprep:prepare(User, saslprep),
    case mnesia:read(pundun_user, User_) of
	[R] ->
	    Record = R#pundun_user{
			    user_details = Details
				   },
	    mnesia:write(Record);
	[] ->
	    {error, user_not_exists};
	Else ->
	    Else
    end.

-spec list_users() ->
    [string()].
list_users()->
    mnesia:dirty_all_keys(pundun_user).

-spec write_admin_user() -> ok.
write_admin_user() ->
    Salt = get_salt(16),
    Normalized = stringprep:prepare("admin", saslprep),
    IterCount = 4096,
    SaltedPassword = scramerl_lib:hi(Normalized, Salt, IterCount),
    Record = #pundun_user{username = "admin",
			  salt = Salt,
			  iteration_count = IterCount,
			  salted_password = SaltedPassword},
    Fun = fun() -> mnesia:write(Record) end,
    {atomic, ok} = transaction(Fun),
    ok.

-spec verify_user(User :: string(), Password :: string()) -> boolean().
verify_user(User, Password) ->
    case mnesia:dirty_read(pundun_user, User) of
	[#pundun_user{salt = Salt, iteration_count = IC,
		      salted_password = SaltedPassword}] ->
	    Normalized = stringprep:prepare(Password, saslprep),
	    case scramerl_lib:hi(Normalized, Salt, IC) of
		SaltedPassword ->
		    true;
		_ ->
		    false
	    end;
	_ ->
	    false
    end.

-spec get_salt(N :: pos_integer()) ->
    [pos_integer()].
get_salt(N) when is_integer(N), N > 0->
    get_salt(N, []).

-spec get_salt(N :: pos_integer(), Acc :: [pos_integer()]) ->
    [pos_integer()].
get_salt(0, Acc) ->
    Acc;
get_salt(N, Acc) ->
    get_salt(N-1, [47 + rand:uniform(78) | Acc]).
