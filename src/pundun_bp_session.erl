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
%% @title Pundun Binary Protocol Sesion Handler.
%% @doc
%% Server side SCRAM Authentication and PBP Session is handled in this
%% module.
%% @end
%%%===================================================================

-module(pundun_bp_session).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
	 init/2,
	 handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
	 respond/2]).

-include("pundun.hrl").
-include("gb_log.hrl").

-define(TIMEOUT, 600000). %% 10 minutes.

-define(WAIT_FOR_CLIENT_FIRST, 0).
-define(WAIT_FOR_CLIENT_FINAL, 1).
-define(AUTHENTICATED, 2).

-record(state, {scram_state = ?WAIT_FOR_CLIENT_FIRST,
		scram_data,
		socket,
		options
		}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Respond to Pundun client with binary data where Pid is the process
%% that handles the session inbetween client and Pundun.
%% @end
%%--------------------------------------------------------------------
-spec respond(Pid :: pid(), Data :: binary()) ->
    ok.
respond(Pid, Data) ->
    gen_server:cast(Pid, {respond, Data}).

%%--------------------------------------------------------------------
%% @doc
%% Pundun Binary Protocol Server Init function that is called when
%% gen_server is started by gen_server:start_link/4.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: [{atom(), term()}]) ->
    {ok, State :: #state{}} |
    {ok, State :: #state{}, Timeout :: pos_integer()} |
    ignore |
    {stop, Reason :: term()}.
init(Args) ->
    Socket = proplists:get_value(socket, Args),
    Opts = proplists:delete(socket, Args),
    {ok, #state{socket = Socket,
		options = Opts}}.

%%--------------------------------------------------------------------
%% @doc
%% Pundun Binary Protocol Server Initialization function that is
%% provided as a callback to mochiweb_socket_server.
%% @end
%%--------------------------------------------------------------------
-spec init(Socket :: port(), Opts :: [{atom(), term()}]) -> ok.
init(Socket, Opts) ->
    ?debug("Initialize pundun binary protocol server: ~p",[[{socket, Socket}|Opts]]),
    State = #state{socket = Socket,
		   options = Opts},
    Timeout = ?TIMEOUT,
    ok = mochiweb_socket:setopts(Socket, [{active, once}]),
    gen_server:enter_loop(?MODULE, [], State, Timeout).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({respond, Data}, State) ->
    ?debug("Responding with ~p", [Data]),
    ok = gen_tcp:send(State#state.socket, Data),
    {noreply, State};
handle_cast(_Msg, State) ->
    ?debug("Unhandled cast message received: ~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({tcp, Socket, Data}, State = #state{scram_state = ?AUTHENTICATED,
						socket = Socket}) ->
    ?debug("Received tcp data: ~p",[Data]),
    spawn_link(pundun_bp_handler, handle_incomming_data, [Data, self()]),
    ok = mochiweb_socket:setopts(Socket, [{active, once}]),
    {noreply, State, ?TIMEOUT};
handle_info({tcp, Socket, Data}, State = #state{scram_state = ?WAIT_FOR_CLIENT_FIRST,
						socket = Socket}) ->
    ?debug("Start SCRAM AUTH: ~p",[Data]),
    {ok, NewState} = handle_client_first_message(Data, State),
    ok = mochiweb_socket:setopts(Socket, [{active, once}]),
    {noreply, NewState, ?TIMEOUT};
handle_info({tcp, Socket, Data}, State = #state{scram_state = ?WAIT_FOR_CLIENT_FINAL,
						socket = Socket}) ->
    ?debug("Handle SCRAM Client Final Message: ~p",[Data]),
    {ok, NewState} = handle_client_final_message(Data, State),
    ok = mochiweb_socket:setopts(Socket, [{active, once}]),
    {noreply, NewState, ?TIMEOUT};
handle_info({ssl, Socket, Data}, State = #state{socket = Socket}) ->
    ?debug("Received ssl data: ~p",[Data]),
    ok = mochiweb_socket:setopts(Socket, [{active, once}]),
    {noreply, State, ?TIMEOUT};
handle_info({tcp_closed, Socket}, State = #state{socket = Socket}) ->
    ?debug("Received tcp_closed, stopping..",[]),
    {stop, normal, State};
handle_info(timeout, State) ->
    ?debug("Timeout occured, stopping..",[]),
    {stop, normal, State};
handle_info(_Info, State) ->
    ?debug("Unhandled Info recieved: ~p",[_Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec handle_client_first_message(Data :: binary(),
				  State :: #state{}) ->
    {ok, State :: #state{}}.
handle_client_first_message(Data, State) when is_binary(Data)->
    ScramData = scramerl_lib:parse(Data),
    ?debug("Parsed SCRAM Data: ~p", [ScramData]),
    Message = maps:get(message, ScramData),
    handle_client_first_message(Message, ScramData, State).

-spec handle_client_first_message(Message :: string(),
				  Data :: map(),
				  State :: #state{}) ->
    {ok, State :: #state{}}.
handle_client_first_message("client-first-message", ScramData, State) ->
    Username = maps:get(username, ScramData),
    {ok, Salt, IterCount} = get_user_salt(Username),

    ClientFirstMsg = maps:get(str, ScramData),
    ClientFirstMsgBare = scramerl_lib:prune('gs2-header', ClientFirstMsg),

    CNonce = maps:get(nonce, ScramData),
    SNonce = scramerl:gen_nonce(),
    Nonce = CNonce ++ SNonce,

    MsgStr = scramerl:server_first_message(Nonce, Salt, IterCount),
    ServerFirstMessage = list_to_binary(MsgStr),
    ?debug("Sending server-first-message: ~p", [ServerFirstMessage]),
    ok = gen_tcp:send(State#state.socket, ServerFirstMessage),

    AddScramData = maps:from_list([{iteration_count, IterCount},
				   {salt, Salt},
				   {snonce, SNonce},
				   {client_first_msg_bare, ClientFirstMsgBare},
				   {server_first_msg, MsgStr}]),
    {ok, State#state{scram_state = ?WAIT_FOR_CLIENT_FINAL,
		     scram_data = maps:merge(ScramData, AddScramData)}};
handle_client_first_message(Message, _ScramData, State) ->
    ?debug("Invalid client first message: ~p", [Message]),
    {ok, State}.

-spec handle_client_final_message(Data :: binary(),
				  State :: #state{}) ->
    {ok, State :: #state{}}.
handle_client_final_message(Data, State) when is_binary(Data)->
    ScramData = scramerl_lib:parse(Data),
    ?debug("Parsed SCRAM Data: ~p", [ScramData]),
    Message = maps:get(message, ScramData),
    handle_client_final_message(Message, ScramData, State).

-spec handle_client_final_message(Message :: string(),
				  Data :: map(),
				  State :: #state{}) ->
    {ok, State :: #state{}}.
handle_client_final_message("client-final-message",
			    ScramData,
			    State = #state{scram_data = PrevScramData}) ->

    Username = maps:get(username, PrevScramData),
    {ok, SaltedPassword} = get_salted_password(Username),

    ClientFirstMsgBare = maps:get(client_first_msg_bare, PrevScramData),
    ServerFirstMsg = maps:get(server_first_msg, PrevScramData),
    ClientFinalMsg = maps:get(str, ScramData),
    CFMWoP = scramerl_lib:prune(proof, ClientFinalMsg),
    AuthMessage = ClientFirstMsgBare ++ "," ++
                  ServerFirstMsg ++ "," ++
		  CFMWoP,
    ?debug("SaltedPassword: ~p",[SaltedPassword]),
    ?debug("AuthMessage: ~p",[AuthMessage]),
    Proof = scramerl:client_proof(SaltedPassword, AuthMessage),

    CheckNonce = maps:get(nonce, ScramData),
    CheckProof = maps:get(proof, ScramData),

    CNonce = maps:get(nonce, PrevScramData),
    SNonce = maps:get(snonce, PrevScramData),
    Nonce = CNonce ++ SNonce,
    AddScramData = maps:from_list([{salted_password, SaltedPassword},
				   {auth_message, AuthMessage}]),
    NewScramData =  maps:merge(PrevScramData, AddScramData),
    NewState = State#state{scram_data = NewScramData},
    handle_client_final_message(Proof, CheckProof, Nonce, CheckNonce, NewState);
handle_client_final_message(Message, _ScramData, State) ->
    ?debug("Invalid client final message: ~p", [Message]),
    {ok, State}.

handle_client_final_message(Proof, Proof,
			    Nonce, Nonce,
			    State = #state{scram_data = ScramData}) ->
    SaltedPassword = maps:get(salted_password, ScramData),
    AuthMessage = maps:get(auth_message, ScramData),
    ServerSignature = scramerl:server_signature(SaltedPassword, AuthMessage),
    MsgStr = scramerl:server_final_message(ServerSignature),
    ServerFirstMessage = list_to_binary(MsgStr),
    gen_tcp:send(State#state.socket, ServerFirstMessage),
    {ok, State#state{scram_state = ?AUTHENTICATED}};
handle_client_final_message(Proof, CheckProof,
			    Nonce, CheckNonce, State) ->
    ?debug("Unmatched Proof ~p =? ~p",[Proof, CheckProof]),
    ?debug("Unmatched Nonce ~p =? ~p",[Nonce, CheckNonce]),
    MsgStr = scramerl:server_final_message({error, "invalid-proof"}),
    ServerFirstMessage = list_to_binary(MsgStr),
    gen_tcp:send(State#state.socket, ServerFirstMessage),
    {ok, State}.

-spec get_user_salt(Username :: string()) ->
    {ok, Salt :: string(), IterCount :: string()} | {error, Reason :: term()}.
get_user_salt(Username) ->
    case mnesia:dirty_read(pundun_user, Username) of
	[#pundun_user{salt = Salt,
		      iteration_count = IterCount}] ->
	    {ok, Salt, IterCount};
	_ ->
	    {error, "unknown-user"}
    end.

-spec get_salted_password(Username :: string()) ->
    {ok, SaltedPassword :: string()}.
get_salted_password(Username) ->
    case mnesia:dirty_read(pundun_user, Username) of
	[#pundun_user{salted_password = SaltedPassword}] ->
	    {ok, SaltedPassword};
	_ ->
	    {error, "unknown-user"}
    end.
