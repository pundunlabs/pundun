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
%% Module Description: Pundun Binary Protocol Handler
%% @end
%%%===================================================================

-module(pundun_bp_handler).

-export([handle_incomming_data/2]).

-include("pundun.hrl").
-include_lib("enterdb/include/enterdb.hrl").
-include_lib("gb_log/include/gb_log.hrl").
-include("APOLLO-PDU-Descriptions.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Handle received binary PBP data.
%% @end
%%--------------------------------------------------------------------
-spec handle_incomming_data(Bin :: binary(), From :: pid()) ->
    ok.
handle_incomming_data(Bin, From) ->
    case 'APOLLO-PDU-Descriptions':decode('APOLLO-PDU', Bin) of
	{ok, PDU} ->
	    ?debug("PDU: ~p", [PDU]),    
	    handle_pdu(PDU, From);
	{error, Reason} ->
	    ?debug("Error decoding received data: ~p", [Reason])
    end.

-spec handle_pdu(PDU :: #'APOLLO-PDU'{}, From :: pid()) ->
    ok.
handle_pdu(#'APOLLO-PDU'{version = Version,
			 transactionId = Tid,
			 procedure = Procedure}, From) ->
    Response = apply_procedure(Procedure),
    send_response(From, Version, Tid, Response).

-spec apply_procedure(Procedure :: {atom(), term()}) ->
    {atom(), term()}.
apply_procedure({createTable, #'CreateTable'{tableName = TabName,
					     keys = KeysDef,
					     tableOptions = TabOptions}})->
    Options = make_options(TabOptions),
    Result = enterdb:create_table(TabName, KeysDef, Options),
    make_response(ok, Result);
apply_procedure({deleteTable, #'DeleteTable'{tableName = TabName}}) ->
    Result = enterdb:delete_table(TabName),
    make_response(ok, Result);
apply_procedure({openTable, #'OpenTable'{tableName = TabName}}) ->
    Result = enterdb:open_table(TabName),
    make_response(ok, Result);
apply_procedure({closeTable, #'CloseTable'{tableName = TabName}}) ->
    Result = enterdb:close_table(TabName),
    make_response(ok, Result);
apply_procedure({tableInfo, #'TableInfo'{tableName = TabName,
					 attributes = asn1_NOVALUE}}) ->
    Result = enterdb:table_info(TabName),
    make_response(proplist, Result);
apply_procedure({tableInfo, #'TableInfo'{tableName = TabName,
					 attributes = Attributes}}) ->
    ValidAttribites = validate_attributes(Attributes, []),
    Result = enterdb:table_info(TabName, ValidAttribites),
    make_response(proplist, Result);
apply_procedure({read, #'Read'{tableName = TabName,
			       key = Key}}) ->
    StripKey = strip_fields(Key),
    ?debug("Read Key: ~p", [StripKey]),
    Result = enterdb:read(TabName, StripKey),
    make_response(columns, Result);
apply_procedure({write, #'Write'{tableName = TabName,
				 key = Key,
			         columns = Columns}}) ->
    StripKey = strip_fields(Key),
    StripColumns = strip_fields(Columns),
    ?debug("Write  ~p:~p", [StripKey,StripColumns]),
    Result = enterdb:write(TabName, StripKey, StripColumns),
    make_response(ok, Result);
apply_procedure({update, #'Update'{tableName = TabName,
				   key = Key,
				   updateOperations = UpdateOperations}}) ->
    StripKey = strip_fields(Key),
    Op = translate_update_operations(UpdateOperations),
    ?debug("Write  ~p:~p", [StripKey, Op]),
    Result = enterdb:update(TabName, StripKey, Op),
    make_response(columns, Result);
apply_procedure({delete, #'Delete'{tableName = TabName,
				   key = Key}}) ->
    StripKey = strip_fields(Key),
    Result = enterdb:delete(TabName, StripKey),
    make_response(ok, Result);
apply_procedure({readRange, #'ReadRange'{tableName = TabName,
					 keyRange = KeyRange,
					 limit = Limit}}) ->
    #'KeyRange'{start = SKey, 'end'= EKey} = KeyRange,
    Start = strip_fields(SKey),
    End = strip_fields(EKey),
    Result = enterdb:read_range(TabName, {Start, End}, Limit),
    make_response(keyColumnsList, Result);
apply_procedure({readRangeN, #'ReadRangeN'{tableName = TabName,
					   startKey = StartKey,
					   n = N}}) ->
    Start = strip_fields(StartKey),
    Result = enterdb:read_range_n(TabName, Start, N),
    make_response(keyColumnsList, Result);
apply_procedure({batchWrite, #'BatchWrite'{tableName = _TabName,
					   deleteKeys = _DeleteKeys,
					   writeKvps = _WriteKvps}}) ->
    {error, #'Error'{cause = {protocol, "function not supported"}}};
apply_procedure({first, #'First'{tableName = TabName}}) ->
    Result = enterdb:first(TabName),
    make_response(kcpIt, Result);
apply_procedure({last, #'Last'{tableName = TabName}}) ->
    Result = enterdb:last(TabName),
    make_response(kcpIt, Result);
apply_procedure({seek, #'Seek'{tableName = TabName,
			       key = Key}}) ->
    Result = enterdb:seek(TabName, strip_fields(Key)),
    make_response(kcpIt, Result);
apply_procedure({next, #'Next'{it = It}}) ->
    Result = enterdb:next(binary_to_term(It)),
    make_response(keyColumnsPair, Result);
apply_procedure({prev, #'Prev'{it = It}}) ->
    Result = enterdb:prev(binary_to_term(It)),
    make_response(keyColumnsPair, Result);
apply_procedure(_) ->
    {error, #'Error'{cause = {protocol, "unknown procedure"}}}.

-spec send_response(To :: pid,
		    Version :: #'Version'{},
		    TransactionId :: integer(),
		    Response :: {atom(), term()}) ->
    ok.
send_response(To, Version, TransactionId, Response) ->
    PDU = #'APOLLO-PDU'{version = Version,
			transactionId = TransactionId,
			procedure = Response},
    ?debug("Response PDU: ~p", [PDU]),
    {ok, Bin} = 'APOLLO-PDU-Descriptions':encode('APOLLO-PDU', PDU),
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
    {response, #'Response'{}};
make_response(columns, {ok, Columns}) ->
    wrap_response({columns, make_seq_of_fields(Columns)});
make_response(keyColumnsPair, {ok, {Key, Value}}) ->
    wrap_response({keyColumnsPair,
		   #'KeyColumnsPair'{key = make_seq_of_fields(Key),
				     columns = make_seq_of_fields(Value)}});
make_response(keyColumnsList, {ok, KVL, Cont}) ->
    List = [#'KeyColumnsPair'{key = make_seq_of_fields(K),
			      columns = make_seq_of_fields(V)} ||
			      {K,V} <- KVL],
    {Complete, Key} =
	case Cont of
	    complete ->
		{true, asn1_NOVALUE};
	    CKey ->
		{false, make_seq_of_fields(CKey)}
	end,
    Continuation = #'Continuation'{complete = Complete,
				   key = Key},
    KeyColumnsList =
	#'KeyColumnsList'{list = List,
			  continuation = Continuation},
    wrap_response({keyColumnsList, KeyColumnsList});
make_response(keyColumnsList, {ok, KVL}) ->
    List = [#'KeyColumnsPair'{key = make_seq_of_fields(K),
			      columns = make_seq_of_fields(V)} ||
			      {K,V} <- KVL],
    KeyColumnsList = #'KeyColumnsList'{list = List},
    wrap_response({keyColumnsList, KeyColumnsList});
make_response(proplist, {ok, List}) ->
    Proplist = [#'Field'{name = atom_to_list(P),
                         value = make_value(A)} || {P,A} <- List],
    wrap_response({propList, Proplist});
make_response(kcpIt, {ok, {Key, Value}, Ref}) ->
    Kcp = #'KeyColumnsPair'{key = make_seq_of_fields(Key),
			    columns = make_seq_of_fields(Value)},
    wrap_response({kcpIt, #'KcpIt'{keyColumnsPair = Kcp,
				   it = term_to_binary(Ref)}});
make_response(_, {error, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[{error, Reason}])),
    Str = lists:sublist(FullStr, ?'maxCauseLength'),
    {error, #'Error'{cause = {system, Str}}};
make_response(_, {badrpc, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[{badrpc, Reason}])),
    Str = lists:sublist(FullStr, ?'maxCauseLength'),
    {error, #'Error'{cause = {system, Str}}}.

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
			    {hash_exlude, [string()]}.

-spec make_options(TabOptions :: [pbp_table_option()]) ->
    [table_option()].
make_options(Options) ->
    make_options(Options, []).

-spec make_options(TabOptions :: [pbp_table_option()],
		   Acc :: [table_option()]) ->
    [table_option()].
make_options([], Acc) ->
    lists:reverse(Acc);
make_options([{type, T} | Rest], Acc) ->
    make_options(Rest, [translate_options({type, T}) | Acc]);
make_options([{dataModel, DT} | Rest], Acc) ->
    make_options(Rest, [{data_model, DT} | Acc]);
make_options([{wrapper, #'Wrapper'{numOfBuckets = NB,
				   timeMargin = TM,
				   sizeMargin = SM}} | Rest], Acc) ->
    EW = #enterdb_wrapper{num_of_buckets = NB,
			  time_margin = asn1_optional(TM),
			  size_margin = asn1_optional(SM)},
    make_options(Rest, [{wrapper, EW} | Acc]);
make_options([{memWrapper, #'Wrapper'{numOfBuckets = NB,
				      timeMargin = TM,
				      sizeMargin = _SM}} | Rest], Acc) ->
    EW = {asn1_optional(TM), NB},
    make_options(Rest, [{mem_wrapper, EW} | Acc]);
make_options([{comparator, C} | Rest], Acc) ->
    make_options(Rest, [{comparator, C} | Acc]);
make_options([{timeSeries, T} | Rest], Acc) ->
    make_options(Rest, [{time_series, T} | Acc]);
make_options([{shards, S} | Rest], Acc) ->
    make_options(Rest, [{shards, S} | Acc]);
make_options([{distributed, B} | Rest], Acc) ->
    make_options(Rest, [{distributed,  B} | Acc]);
make_options([{replicationFactor, RF} | Rest], Acc) ->
    make_options(Rest, [{replication_factor,  RF} | Acc]);
make_options([{hashExclude, HE} | Rest], Acc) ->
    make_options(Rest, [{hash_exclude,  HE} | Acc]);
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
    {double, <<V:64/float>>};
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
strip_fields([#'Field'{name = N, value = {string, Str}} | Rest], Acc) ->
    strip_fields(Rest, [{N, binary_to_list(Str)} | Acc]);
strip_fields([#'Field'{name = N, value = {double, Bin}} | Rest], Acc) ->
     <<D:64/float>> = Bin,
     strip_fields(Rest, [{N, D} | Acc]);
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

-spec asn1_optional(A :: undefined |
			 asn1_NOVALUE |
			 term()) ->
    A :: term() | asn1_NOVALUE | undefined.
asn1_optional(undefined) ->
    asn1_NOVALUE;
asn1_optional(asn1_NOVALUE) ->
    undefined;
asn1_optional(A) ->
    A.

-spec translate_options(PBP_Option :: pbp_table_option()) ->
    Option :: table_option().
translate_options({type, etsLeveldb}) ->
    {type, ets_leveldb};
translate_options({type, leveldbWrapped}) ->
    {type, leveldb_wrapped};
translate_options({type, etsLeveldbWrapped}) ->
    {type, ets_leveldb_wrapped};
translate_options(Option) ->
    Option.

-spec translate_update_operations(UpdateOperations :: [#'UpdateOperation'{}])->
    update_op().
translate_update_operations(UpdateOperations) ->
    translate_update_operations(UpdateOperations, []).

-spec translate_update_operations(UpdateOperations :: [#'UpdateOperation'{}],
				  Acc :: update_op())->
    update_op().
translate_update_operations([UpOp | Rest], Acc) ->
    #'UpdateOperation'{field = F,
		       updateInstruction = UpInst,
		       value = {_, V},
		       defaultValue = DefaultValue} = UpOp,
    I = translate_update_instruction(UpInst),
    InitList =
	case DefaultValue of
	    {_, DV} ->
		[{1,F}, {2, I}, {3, V}, {4, DV}];
	    asn1_NOVALUE ->
		[{1,F}, {2, I}, {3, V}]
	end,
    Tuple = erlang:make_tuple(length(InitList), undefined, InitList),
    translate_update_operations(Rest, [Tuple | Acc]);
translate_update_operations([], Acc) ->
    lists:reverse(Acc).

-spec translate_update_instruction(UpInst :: #'UpdateInstruction'{}) ->
    update_instruction().
translate_update_instruction(#'UpdateInstruction'{instruction = increment,
						  treshold = undefined,
						  setValue = undefined}) ->
    increment;
translate_update_instruction(#'UpdateInstruction'{instruction = increment,
						  treshold = Treshold,
						  setValue = SetValue}) ->
    {increment, Treshold, SetValue};
translate_update_instruction(#'UpdateInstruction'{instruction = overwrite}) ->
    overwrite.
