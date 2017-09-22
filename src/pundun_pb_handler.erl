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
					      table_options = TabOptions}})->
    Options = make_options(TabOptions),
    Result = enterdb:create_table(TabName, KeysDef, Options),
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
apply_procedure({update, #'Update'{table_name = TabName,
				    key = Key,
				    update_operation = UpdateOperation}}) ->
    StripKey = strip_fields(Key),
    Op = translate_update_operation(UpdateOperation),
    ?debug("Update  ~p:~p", [StripKey, Op]),
    Result = enterdb:update(TabName, StripKey, Op),
    make_response(columns, Result);
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
    Result = enterdb:next(It),
    make_response(key_columns_pair, Result);
apply_procedure({prev, #'Prev'{it = It}}) ->
    Result = enterdb:prev(It),
    make_response(key_columns_pair, Result);
apply_procedure({add_index, #'AddIndex'{table_name = TabName,
					config = Config}}) ->
    IndexConfig = make_index_config(Config),
    Result = enterdb:add_index(TabName, IndexConfig),
    make_response(ok, Result);
apply_procedure({remove_index, #'RemoveIndex'{table_name = TabName,
					      columns = Columns}}) ->
    Result = enterdb:remove_index(TabName, Columns),
    make_response(ok, Result);
apply_procedure({index_read, #'IndexRead'{table_name = TabName,
					  column_name = ColumnName,
					  term = Term,
					  limit = Limit}}) ->
    Result = enterdb:index_read(TabName, ColumnName, Term, Limit),
    make_response(postings, Result);
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
    Bin = apollo_pb:encode_msg(PDU),
    pundun_bp_session:respond(To, Bin).

-spec make_response(Choice :: ok |
			      value |
			      key_columns_pair |
			      key_columns_list |
			      proplist |
			      kcpIt |
			      keys,
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
    wrap_response({columns, make_fields(Columns)});
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
    Fields = [#'Field'{name = atom_to_list(P),
		       value = make_value(A)} || {P,A} <- List],
    Proplist = #'Fields'{fields = Fields},
    wrap_response({proplist, Proplist});
make_response(kcp_it, {ok, {Key, Value}, Ref}) ->
    Kcp = #'KeyColumnsPair'{key = make_seq_of_fields(Key),
			    columns = make_seq_of_fields(Value)},
    wrap_response({kcp_it, #'KcpIt'{key_columns_pair = Kcp,
				   it = Ref}});
make_response(postings, {ok, Postings}) ->
    List = [#'Posting'{key = make_seq_of_fields(Key),
		       timestamp = Ts,
		       frequency = Freq,
		       position = Pos} ||
		#{freq := Freq,
		  key := Key,
		  pos := Pos,
		  ts := Ts} <- Postings],
    wrap_response({postings, #'Postings'{list = List}});
make_response(_, {error, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[{error, Reason}])),
    {error, #'Error'{cause = {system, FullStr}}};
make_response(_, {badrpc, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[{badrpc, Reason}])),
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
			    {num_of_shards, integer()} |
			    {distributed, boolean()} |
			    {replication_factor, integer()} |
			    {hash_exclude, [string()]}.

-spec make_options(TabOptions :: [#'TableOption'{}]) ->
    [table_option()].
make_options([#'TableOption'{}|_] = L) ->
    Options = [O || #'TableOption'{opt = O} <- L],
    make_options(Options, []);
make_options([]) ->
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
make_options([{tda, T} | Rest], Acc) ->
    Tda = translate_tda(T),
    make_options(Rest, [{tda, Tda} | Acc]);
make_options([{ttl, T} | Rest], Acc) ->
    Ttl = translate_ttl(T),
    make_options(Rest, [{ttl, Ttl} | Acc]);
make_options([{comparator, C} | Rest], Acc) ->
    make_options(Rest, [translate_options({comparator, C}) | Acc]);
make_options([{time_series, T} | Rest], Acc) ->
    make_options(Rest, [{time_series, T} | Acc]);
make_options([{num_of_shards, S} | Rest], Acc) ->
    make_options(Rest, [{num_of_shards, S} | Acc]);
make_options([{distributed, D} | Rest], Acc) ->
    make_options(Rest, [{distributed, D} | Acc]);
make_options([{replication_factor, R} | Rest], Acc) ->
    make_options(Rest, [{replication_factor, R} | Acc]);
make_options([{hash_exclude, #'FieldNames'{field_names = FN}} | Rest], Acc) ->
    make_options(Rest, [{hash_excude, FN} | Acc]);
make_options([_ | Rest], Acc) ->
    make_options(Rest, Acc).

-spec make_fields(Key :: [{string(), term()}]) ->
    #'Fields'{}.
make_fields(Key) when is_list(Key)->
    Fields = [#'Field'{name = Name, value = make_value(Value)}
		|| {Name, Value} <- Key],
    #'Fields'{fields = Fields};
make_fields(Else)->
    ?debug("Invalid key: ~p",[Else]),
    Else.

-spec make_seq_of_fields(Key :: [{string(), term()}]) ->
    [#'Fields'{}].
make_seq_of_fields(Key) when is_list(Key)->
    [#'Field'{name = Name, value = make_value(Value)}
	|| {Name, Value} <- Key];
make_seq_of_fields(Else)->
    ?debug("Invalid key: ~p",[Else]),
    Else.

-spec make_value(V :: term()) ->
    {boolean, Bool :: true | false} |
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
    {boolean, true};
make_value(false) ->
    {boolean, false};
make_value(V) when is_list(V) ->
    case is_list_of_printables(V) of
	{true, L} ->
	    {string, L};
	false ->
	    L = io_lib:format("~p",[V]),
	    {string, lists:flatten(L)}
    end;
make_value(undefined) ->
    {null, <<>>};
make_value(A) when is_atom(A) ->
    {string, atom_to_list(A)};
make_value(T) when is_tuple(T) ->
    {binary, term_to_binary(T)}.

is_list_of_printables(L) ->
    case io_lib:printable_unicode_list(L) of
	true -> {true, L};
	false ->
	    case io_lib:printable_unicode_list(lists:flatten(L)) of
		true ->
		    {true, lists:flatten([E++" "||E <- L])};
		false -> false
	    end
    end.

-spec strip_fields(Fields :: [#'Field'{}]) ->
    [{string(), term()}].
strip_fields(Fields) ->
        strip_fields(Fields, []).

-spec strip_fields(Fields :: [#'Field'{}],
		   Acc :: [{string(), term()}]) ->
    [{string(), term()}].
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
    strip_fields(Rest, [{N, V} | Acc]);
strip_fields([], Acc) ->
    lists:reverse(Acc).

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
validate_attributes(["num_of_shards" | T], Acc) ->
    validate_attributes(T, [num_of_shards | Acc]);
validate_attributes(["nodes" | T], Acc) ->
    validate_attributes(T, [nodes | Acc]);
validate_attributes([_H | T], Acc) ->
    validate_attributes(T, Acc).

-spec translate_options(PBP_Option :: pbp_table_option()) ->
    Option :: table_option().
translate_options({type, 'ROCKSDB'}) ->
    {type, rocksdb};
translate_options({type, 'LEVELDB'}) ->
    {type, leveldb};
translate_options({type, 'MEMLEVELDB'}) ->
    {type, ets_leveldb};
translate_options({type, 'LEVELDBWRAPPED'}) ->
    {type, leveldb_wrapped};
translate_options({type, 'MEMLEVELDBWRAPPED'}) ->
    {type, ets_leveldb_wrapped};
translate_options({type, 'LEVELDBTDA'}) ->
    {type, leveldb_tda};
translate_options({type, 'MEMLEVELDBTDA'}) ->
    {type, ets_leveldb_tda};
translate_options({data_model, 'KV'}) ->
    {data_model, kv};
translate_options({data_model, 'ARRAY'}) ->
    {data_model, array};
translate_options({data_model, 'MAP'}) ->
    {data_model, map};
translate_options({comparator, 'DESCENDING'}) ->
    {comparator, descending};
translate_options({comparator, 'ASCENDING'}) ->
    {comparator, ascending};
translate_options({hashing_method, 'VIRTUALNODES'}) ->
    {hashing_method, virtual_nodes};
translate_options({hashing_method, 'CONSISTENT'}) ->
    {hashing_method, consistent};
translate_options({hashing_method, 'UNIFORM'}) ->
    {hashing_method, uniform};
translate_options({hashing_method, 'RENDEZVOUS'}) ->
    {hashing_method, rendezvous};
translate_options(Option) ->
    Option.

-spec translate_wrapper(#'Wrapper'{} | undefined) ->
    #{} | undefined.
translate_wrapper(#'Wrapper'{num_of_buckets = NB,
			     time_margin = TM,
			     size_margin = SM}) ->
    #{num_of_buckets => NB,
      time_margin => TM,
      size_margin => SM};
translate_wrapper(undefined) ->
    undefined.

-spec translate_tda(#'Tda'{} | undefined) ->
    #{} | undefined.
translate_tda(#'Tda'{num_of_buckets = NB,
		     time_margin = TM,
		     ts_field = TsF,
		     precision = Precision}) ->
    #{num_of_buckets => NB, time_margin => TM, ts_field => TsF,
      precision => translate_time_unit(Precision)};
translate_tda(undefined) ->
    undefined.

-spec translate_ttl(TTL :: term()) ->
    T :: integer().
translate_ttl(TTL) when is_integer(TTL) ->
    TTL;
translate_ttl(_) ->
    0.

-spec translate_time_unit(Precision :: 'SECOND' | 'MILLISECOND' |
				       'MICROSECOND' | 'NANOSECOND') ->
    second | millisecond | microsecond | nanosecond.
translate_time_unit('SECOND') -> second;
translate_time_unit('MILLISECOND') -> millisecond;
translate_time_unit('MICROSECOND') -> microsecond;
translate_time_unit('NANOSECOND') -> nanosecond.

-spec translate_update_operation(UpdateOperation :: [#'UpdateOperation'{}]) ->
    term().
translate_update_operation(UpdateOperation) ->
    translate_update_operation(UpdateOperation, []).

-spec translate_update_operation(UpdateOperation :: [#'UpdateOperation'{}],
				 Acc :: term()) ->
    term().
translate_update_operation([UpOp | Rest], Acc) ->
    #'UpdateOperation'{field = F,
		       update_instruction  = UpInst,
		       value = Value,
		       default_value = DefaultValue} = UpOp,
    I = translate_update_instruction(UpInst),
    V = translate_value(Value),
    InitList =
	case DefaultValue of
	    #'Value'{} ->
		DV = translate_value(DefaultValue),
		[{1,F}, {2, I}, {3, V}, {4, DV}];
	    undefined ->
		[{1,F}, {2, I}, {3, V}]
	end,
    Tuple = erlang:make_tuple(length(InitList), undefined, InitList),
    translate_update_operation(Rest, [Tuple | Acc]);
translate_update_operation([], Acc) ->
    lists:reverse(Acc).

-spec translate_value(#'Value'{} | undefined) ->
    term().
translate_value(#'Value'{value = {null, _}}) ->
    undefined;
translate_value(#'Value'{value = {_, Term}}) ->
    Term.

-spec translate_update_instruction(UpInst :: #'UpdateInstruction'{}) ->
    term().
translate_update_instruction(#'UpdateInstruction'{instruction = 'INCREMENT',
						  threshold = <<>>,
						  set_value = <<>>}) ->
    increment;
translate_update_instruction(#'UpdateInstruction'{instruction = 'INCREMENT',
						  threshold = Threshold,
						  set_value = SetValue}) ->
    {increment,
     binary:decode_unsigned(Threshold, big),
     binary:decode_unsigned(SetValue, big)};
translate_update_instruction(#'UpdateInstruction'{instruction = 'OVERWRITE'}) ->
    overwrite.

make_index_config(List) ->
    make_index_config(List, []).

make_index_config([#'IndexConfig'{column = C, options = Opts} | Rest], Acc) ->
    make_index_config(Rest, [{C, make_index_options(Opts)} | Acc]);
make_index_config([], Acc) ->
    lists:reverse(Acc).

make_index_options(#'IndexOptions'{char_filter = CF,
				   tokenizer = Tokenizer,
				   token_filter = TokenFilter}) ->
    M1 = translate_char_filter(CF, #{}),
    M2 = translate_tokeizer(Tokenizer, M1),
    make_token_filter(TokenFilter, M2).

translate_char_filter('NFC', M) ->
    M#{char_filter => nfc};
translate_char_filter('NFD', M) ->
    M#{char_filter => nfd};
translate_char_filter('NFKC', M) ->
    M#{char_filter => nfkc};
translate_char_filter('NFKD', M) ->
    M#{char_filter => nfkd};
translate_char_filter(_, M) ->
    M.

translate_tokeizer('UNICODE_WORD_BOUNDARIES', M) ->
    M#{tokenizer => unicode_word_boundaries};
translate_tokeizer(_, M) ->
    M.

make_token_filter(#'TokenFilter'{transform = Transform,
				 add = Add,
				 delete = Delete,
				 stats = Stats}, M) ->
    M1 = translate_transform(Transform, #{}),
    M2 = make_add_words(Add, M1),
    M3 = make_delete_words(Delete, M2),
    M4 = translate_stats(Stats, M3),
    M#{token_filter => M4};
make_token_filter(_, M) ->
    M.

translate_transform('LOWERCASE', M) ->
    M#{transform => lowercase};
translate_transform('UPPERCASE', M) ->
    M#{transform => uppercase};
translate_transform('CASEFOLD', M) ->
    M#{transform => casefold};
translate_transform(_, M) ->
    M.

make_add_words([], M)->
    M;
make_add_words(Add, M)->
    M#{add => Add}.

make_delete_words([], M) ->
    M;
make_delete_words(Delete, M) ->
    M#{delete => [handle_stopword(W) || W <- Delete]}.

handle_stopword("$english_stopwords") ->
    english_stopwords;
handle_stopword("$lucene_stopwords") ->
    lucene_stopwords;
handle_stopword("$wikipages_stopwords") ->
    wikipages_stopwords;
handle_stopword(W) ->
    W.

translate_stats('UNIQUE', M) ->
    M#{stats => unique};
translate_stats('FREQUENCY', M) ->
    M#{stats => freqs};
translate_stats('POSITION', M) ->
    M#{stats => position};
translate_stats('NOSTATS', M) ->
    M.
