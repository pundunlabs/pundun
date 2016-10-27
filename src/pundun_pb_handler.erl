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
%% @doc
%% Module Description: Pundun Protocol Buffers Handler
%% @end
%%%===================================================================

-module(pundun_pb_handler).

-export([handle_incomming_data/2]).

-include("pundun.hrl").
-include_lib("enterdb/include/enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").
-include("apollo_pb.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Handle received binary PBP data.
%% @end
%%--------------------------------------------------------------------
-spec handle_incomming_data(Bin :: binary(), From :: pid()) ->
    ok.
handle_incomming_data(Bin, From) ->
    case apollo_pb:decode_msg(Bin, 'ApolloPdu') of
	#'ApolloPdu'{} = PDU ->
	    ?debug("PDU: ~p", [PDU]),    
	    handle_pdu(PDU, From);
	{error, Reason} ->
	    ?debug("Error decoding received data: ~p", [Reason])
    end.

-spec handle_pdu(PDU :: #'ApolloPdu'{}, From :: pid()) ->
    ok.
handle_pdu(#'ApolloPdu'{version = Version,
			transaction_id = Tid,
			procedure = Procedure},
	   From) ->
    Response = apply_procedure(Procedure),
    send_response(From, Version, Tid, Response).

-spec apply_procedure(Procedure :: {atom(), term()}) ->
    {atom(), term()}.
apply_procedure({create_table, #'CreateTable'{table_name = TabName,
					      keys = KeysDef,
					      columns = ColumnsDef,
					      indexes = IndexesDef,
					      table_options = TabOptions}})->
    Options = make_options(TabOptions),
    Result = enterdb:create_table(TabName, KeysDef, ColumnsDef,
				  IndexesDef, Options),
    make_response(ok, Result);
apply_procedure({delete_table, #'DeleteTable'{table_name = TabName}}) ->
    Result = enterdb:delete_table(TabName),
    make_response(ok, Result);
apply_procedure({open_table, #'OpenTable'{table_name = TabName}}) ->
    Result = enterdb:open_table(TabName),
    make_response(ok, Result);
apply_procedure({close_table, #'CloseTable'{table_name = TabName}}) ->
    Result = enterdb:close_table(TabName),
    make_response(ok, Result);
apply_procedure({table_info, #'TableInfo'{table_name = TabName,
					  attributes = []}}) ->
    Result = enterdb:table_info(TabName),
    make_response(proplist, Result);
apply_procedure({table_info, #'TableInfo'{table_name = TabName,
					 attributes = Attributes}}) ->
    ValidAttribites = validate_attributes(Attributes, []),
    Result = enterdb:table_info(TabName, ValidAttribites),
    make_response(proplist, Result);
apply_procedure({read, #'Read'{table_name = TabName,
			       key = Key}}) ->
    StripKey = strip_fields(Key),
    ?debug("Read Key: ~p", [StripKey]),
    Result = enterdb:read(TabName, StripKey),
    make_response(columns, Result);
apply_procedure({write, #'Write'{table_name = TabName,
				 key = Key,
			         columns = Columns}}) ->
    StripKey = strip_fields(Key),
    StripColumns = strip_fields(Columns),
    ?debug("Write  ~p:~p", [StripKey,StripColumns]),
    Result = enterdb:write(TabName, StripKey, StripColumns),
    make_response(ok, Result);
apply_procedure({delete, #'Delete'{table_name = TabName,
				   key = Key}}) ->
    StripKey = strip_fields(Key),
    Result = enterdb:delete(TabName, StripKey),
    make_response(ok, Result);
apply_procedure({read_range, #'ReadRange'{table_name = TabName,
					  start_key = SKey,
					  end_key = EKey,
					  limit = Limit}}) ->
    Start = strip_fields(SKey),
    End = strip_fields(EKey),
    Result = enterdb:read_range(TabName, {Start, End}, Limit),
    make_response(key_columns_list, Result);
apply_procedure({read_range_n, #'ReadRangeN'{table_name = TabName,
					     start_key = StartKey,
					     n = N}}) ->
    Start = strip_fields(StartKey),
    Result = enterdb:read_range_n(TabName, Start, N),
    make_response(key_columns_list, Result);
apply_procedure({batch_write, #'BatchWrite'{table_name = _TabName,
					    delete_keys = _DeleteKeys,
					    write_kvps = _WriteKvps}}) ->
    {error, #'Error'{cause = {protocol, "function not supported"}}};
apply_procedure({first, #'First'{table_name = TabName}}) ->
    Result = enterdb:first(TabName),
    make_response(kcp_it, Result);
apply_procedure({last, #'Last'{table_name = TabName}}) ->
    Result = enterdb:last(TabName),
    make_response(kcp_it, Result);
apply_procedure({seek, #'Seek'{table_name = TabName,
			       key = Key}}) ->
    Result = enterdb:seek(TabName, strip_fields(Key)),
    make_response(kcp_it, Result);
apply_procedure({next, #'Next'{it = It}}) ->
    Result = enterdb:next(binary_to_term(It)),
    make_response(key_columns_pair, Result);
apply_procedure({prev, #'Prev'{it = It}}) ->
    Result = enterdb:prev(binary_to_term(It)),
    make_response(key_columns_pair, Result);
apply_procedure(_) ->
    {error, #'Error'{cause = {protocol, "unknown procedure"}}}.

-spec send_response(To :: pid,
		    Version :: #'Version'{},
		    TransactionId :: integer(),
		    Response :: {atom(), term()}) ->
    ok.
send_response(To, Version, TransactionId, Response) ->
    PDU = #'ApolloPdu'{version = Version,
		       transaction_id = TransactionId,
		       procedure = Response},
    ?debug("Response PDU: ~p", [PDU]),
    {ok, Bin} = apollo_pb:encode_msg(PDU),
    pundun_bp_session:respond(To, Bin).

-spec make_response(Choice :: ok |
			      value |
			      keyColumnsPair |
			      keyColumnsList |
			      proplist |
			      kcpIt,
		    Result :: ok |
			      {ok, value()} |
			      {ok, [{atom(), term()}]} |
			      {ok, [kvp()], complete} |
			      {ok, [kvp()], key()} |
			      {ok, [kvp()]} |
			      {ok, kvp(), pid()} |
			      {error, term()}) ->
    {response, #'Response'{}} |
    {error, #'Error'{}}.
make_response(ok, ok) ->
    {response, #'Response'{result = {ok, "ok"}}};
make_response(columns, {ok, Columns}) ->
    wrap_response({columns, make_seq_of_fields(Columns)});
make_response(key_columns_pair, {ok, {Key, Value}}) ->
    wrap_response({key_columns_pair,
		   #'KeyColumnsPair'{key = make_seq_of_fields(Key),
				     columns = make_seq_of_fields(Value)}});
make_response(key_columns_list, {ok, KVL, Cont}) ->
    List = [#'KeyColumnsPair'{key = make_seq_of_fields(K),
			      columns = make_seq_of_fields(V)} ||
			      {K,V} <- KVL],
    {Complete, Key} =
	case Cont of
	    complete ->
		{true, []};
	    CKey ->
		{false, make_seq_of_fields(CKey)}
	end,
    Continuation = #'Continuation'{complete = Complete,
				   key = Key},
    KeyColumnsList =
	#'KeyColumnsList'{list = List,
			  continuation = Continuation},
    wrap_response({key_columns_list, KeyColumnsList});
make_response(key_columns_list, {ok, KVL}) ->
    List = [#'KeyColumnsPair'{key = make_seq_of_fields(K),
			      columns = make_seq_of_fields(V)} ||
			      {K,V} <- KVL],
    KeyColumnsList = #'KeyColumnsList'{list = List},
    wrap_response({key_columns_list, KeyColumnsList});
make_response(proplist, {ok, List}) ->
    Proplist = [#'Field'{name = atom_to_list(P),
                         value = make_value(A)} || {P,A} <- List],
    wrap_response({proplist, Proplist});
make_response(kcp_it, {ok, {Key, Value}, Ref}) ->
    Kcp = #'KeyColumnsPair'{key = make_seq_of_fields(Key),
			    columns = make_seq_of_fields(Value)},
    wrap_response({kcp_it, #'KcpIt'{key_columns_pair = Kcp,
				   it = term_to_binary(Ref)}});
make_response(_, {error, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[Reason])),
    {error, #'Error'{cause = {system, FullStr}}}.

-spec wrap_response(Term :: term()) ->
    {response, #'Response'{}}.
wrap_response(Term) ->
    {response, #'Response'{result = Term}}.

-type pbp_table_option() :: {type, type()} |
			    {dataModel, data_model()} |
			    {wrapper, #'Wrapper'{}} |
			    {memWrapper, #'Wrapper'{}} |
			    {comparator, comparator()} |
			    {timeSeries, boolean()} |
			    {shards, integer()} |
			    {distributed, boolean()} |
			    {replication_factor, integer()} |
			    {hash_exclude, [string()]}.

-spec make_options(TabOptions :: [#'TableOption'{}]) ->
    [table_option()].
make_options([#'TableOption'{}|_] = L) ->
    Options = [O || #'TableOption'{opt = O} <- L],
    make_options(Options, []);
make_options(undefined) ->
    [].

-spec make_options(TabOptions :: [pbp_table_option()],
		   Acc :: [table_option()]) ->
    [table_option()].
make_options([], Acc) ->
    lists:reverse(Acc);
make_options([{type, T} | Rest], Acc) ->
    make_options(Rest, [translate_options({type, T}) | Acc]);
make_options([{dataModel, DT} | Rest], Acc) ->
    make_options(Rest, [translate_options({data_model, DT}) | Acc]);
make_options([{wrapper, W} | Rest], Acc) ->
    EW = translate_wrapper(W),
    make_options(Rest, [{wrapper, EW} | Acc]);
make_options([{memWrapper, W} | Rest], Acc) ->
    EW = translate_wrapper(W),
    make_options(Rest, [{mem_wrapper, EW} | Acc]);
make_options([{comparator, C} | Rest], Acc) ->
    make_options(Rest, [translate_options({comparator, C}) | Acc]);
make_options([{time_series, T} | Rest], Acc) ->
    make_options(Rest, [{time_series, T} | Acc]);
make_options([{shards, S} | Rest], Acc) ->
    make_options(Rest, [{shards, S} | Acc]);
make_options([{distributed, D} | Rest], Acc) ->
    make_options(Rest, [{distributed, D} | Acc]);
make_options([{replication_factor, R} | Rest], Acc) ->
    make_options(Rest, [{replication_factor, R} | Acc]);
make_options([{hash_exclude, #'FieldNames'{field_names = FN}} | Rest], Acc) ->
    make_options(Rest, [{hash_excude, FN} | Acc]);
make_options([_ | Rest], Acc) ->
    make_options(Rest, Acc).

-spec make_seq_of_fields(Key :: [{string(), term()}]) ->
    [#'Field'{}].
make_seq_of_fields(Key) when is_list(Key)->
    [#'Field'{name = Name, value = make_value(Value)}
	|| {Name, Value} <- Key];
make_seq_of_fields(Else)->
    ?debug("Invalid key: ~p",[Else]),
    Else.

-spec make_value(V :: term()) ->
    {bool, Bool :: true | false} |
    {int, Int :: integer()} |
    {binary, Bin :: binary()} |
    {null, Null :: undefined} |
    {double, Double :: binary()} |
    {binary, Binary :: binary()} |
    {string, Str :: [integer()]}.
make_value(V) when is_binary(V) ->
    {binary, V};
make_value(V) when is_integer(V) ->
    {int, V};
make_value(V) when is_float(V) ->
    {double, V};
make_value(true) ->
    {bool, true};
make_value(false) ->
    {bool, false};
make_value(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
	true ->
	    {string, V};
	false ->
	    {binary, list_to_binary(V)}
    end;
make_value(undefined) ->
    {null, 'NULL'};
make_value(A) when is_atom(A) ->
    {string, atom_to_list(A)};
make_value(T) when is_tuple(T) ->
    {binary, term_to_binary(T)}.

-spec strip_fields(Fields :: [#'Field'{}]) ->
    [{string(), term()}].
    strip_fields(Fields) ->
        strip_fields(Fields, []).

-spec strip_fields(Fields :: [#'Field'{}],
		   Acc :: [{string(), term()}]) ->
    [{string(), term()}].
strip_fields([], Acc) ->
    lists:reverse(Acc);
strip_fields([#'Field'{name = N, value = {boolean, B}} | Rest], Acc) ->
    Bool =
	case B of
	    0 -> false;
	    1 -> true;
	    B -> B
	end,
    strip_fields(Rest, [{N, Bool} | Acc]);
strip_fields([#'Field'{name = N, value = {null, _}} | Rest], Acc) ->
    strip_fields(Rest, [{N, undefined} | Acc]);
strip_fields([#'Field'{name = N, value = {_, V}} | Rest], Acc) ->
    strip_fields(Rest, [{N, V} | Acc]).

-spec validate_attributes(Attr :: [string()], Acc :: [atom()]) ->
    [atom()].
validate_attributes([], Acc) ->
    lists:reverse(Acc);
validate_attributes(["name" | T], Acc) ->
    validate_attributes(T, [name | Acc]);
validate_attributes(["key" | T], Acc) ->
    validate_attributes(T, [key | Acc]);
validate_attributes(["columns" | T], Acc) ->
    validate_attributes(T, [columns | Acc]);
validate_attributes(["indexes" | T], Acc) ->
    validate_attributes(T, [indexes | Acc]);
validate_attributes(["comparator" | T], Acc) ->
    validate_attributes(T, [comparator | Acc]);
validate_attributes(["time_ordered" | T], Acc) ->
    validate_attributes(T, [time_ordered | Acc]);
validate_attributes(["wrapped" | T], Acc) ->
    validate_attributes(T, [wrapped | Acc]);
validate_attributes(["mem_wrapped" | T], Acc) ->
    validate_attributes(T, [mem_wrapped | Acc]);
validate_attributes(["type" | T], Acc) ->
    validate_attributes(T, [type | Acc]);
validate_attributes(["data_model" | T], Acc) ->
    validate_attributes(T, [data_model | Acc]);
validate_attributes(["shards" | T], Acc) ->
    validate_attributes(T, [shards | Acc]);
validate_attributes(["nodes" | T], Acc) ->
    validate_attributes(T, [nodes | Acc]);
validate_attributes([_H | T], Acc) ->
    validate_attributes(T, Acc).

-spec translate_options(PBP_Option :: pbp_table_option()) ->
    Option :: table_option().
translate_options({type, 'LEVELDB'}) ->
    {type, leveldb};
translate_options({type, 'ETSLEVELDB'}) ->
    {type, ets_leveldb};
translate_options({type, 'LEVELDBWRAPPED'}) ->
    {type, leveldb_wrapped};
translate_options({type, 'ETSLEVELDBWRAPPED'}) ->
    {type, ets_leveldb_wrapped};
translate_options({data_model, 'BINARY'}) ->
    {data_model, binary};
translate_options({data_model, 'ARRAY'}) ->
    {data_model, array};
translate_options({data_model, 'HASH'}) ->
    {data_model, hash};
translate_options({comparator, 'DESCENDING'}) ->
    {comparator, descending};
translate_options({comparator, 'ASCENDING'}) ->
    {comparator, ascending};
translate_options(Option) ->
    Option.

-spec translate_wrapper(#'Wrapper'{} | undefined) ->
    #enterdb_wrapper{} | undefined.
translate_wrapper(#'Wrapper'{num_of_buckets = NB,
			     time_margin = TM,
			     size_margin = SM}) ->
    #enterdb_wrapper{num_of_buckets = NB,
		     time_margin = TM,
		     size_margin = SM};
translate_wrapper(undefined) ->
    undefined.
