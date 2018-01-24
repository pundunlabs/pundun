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

%%--------------------------------------------------------------------
%% @doc
%% Handle received binary PBP data.
%% @end
%%--------------------------------------------------------------------
-spec handle_incomming_data(Bin :: binary(), From :: pid()) ->
    ok.
handle_incomming_data(Bin, From) ->
    case apollo_pb:decode_msg(Bin, 'ApolloPdu') of
	#{} = PDU ->
	    ?debug("PDU: ~p", [PDU]),
	    handle_pdu(PDU, From);
	{error, Reason} ->
	    ?debug("Error decoding received data: ~p", [Reason])
    end.

-spec handle_pdu(PDU :: #{}, From :: pid()) ->
    ok.
handle_pdu(#{version := Version,
	     transaction_id := Tid,
	     procedure := Procedure},
	   From) ->
    Response = apply_procedure(Procedure),
    send_response(From, Version, Tid, Response).

-spec apply_procedure(Procedure :: {atom(), term()}) ->
    {atom(), term()}.
apply_procedure({create_table, #{table_name := TabName,
				 keys := KeysDef,
				 table_options := TabOptions}})->
    Options = make_options(TabOptions),
    Result = enterdb:create_table(TabName, KeysDef, Options),
    make_response(ok, Result);
apply_procedure({delete_table, #{table_name := TabName}}) ->
    Result = enterdb:delete_table(TabName),
    make_response(ok, Result);
apply_procedure({open_table, #{table_name := TabName}}) ->
    Result = enterdb:open_table(TabName),
    make_response(ok, Result);
apply_procedure({close_table, #{table_name := TabName}}) ->
    Result = enterdb:close_table(TabName),
    make_response(ok, Result);
apply_procedure({table_info, #{table_name := TabName,
			       attributes := []}}) ->
    Result = enterdb:table_info(TabName),
    make_response(proplist, Result);
apply_procedure({table_info, #{table_name := TabName,
			       attributes := Attributes}}) ->
    ValidAttribites = validate_attributes(Attributes, []),
    Result = enterdb:table_info(TabName, ValidAttribites),
    make_response(proplist, Result);
apply_procedure({read, #{table_name := TabName,
			 key := Key}}) ->
    StripKey = strip_fields(Key),
    ?debug("Read Key: ~p", [StripKey]),
    Result = enterdb:read(TabName, StripKey),
    make_response(columns, Result);
apply_procedure({write, #{table_name := TabName,
			  key := Key,
			  columns := Columns}}) ->
    StripKey = strip_fields(Key),
    StripColumns = strip_fields(Columns),
    ?debug("Write  ~p:~p -> ~p", [StripKey, StripColumns, TabName]),
    Result = enterdb:write(TabName, StripKey, StripColumns),
    make_response(ok, Result);
apply_procedure({update, #{table_name := TabName,
			   key := Key,
			   update_operation := UpdateOperation}}) ->
    StripKey = strip_fields(Key),
    Op = translate_update_operation(UpdateOperation),
    ?debug("Update  ~p:~p", [StripKey, Op]),
    Result = enterdb:update(TabName, StripKey, Op),
    make_response(columns, Result);
apply_procedure({delete, #{table_name := TabName,
			   key := Key}}) ->
    StripKey = strip_fields(Key),
    Result = enterdb:delete(TabName, StripKey),
    make_response(ok, Result);
apply_procedure({read_range, #{table_name := TabName,
			       start_key := SKey,
			       end_key := EKey,
			       limit := Limit}}) ->
    Start = strip_fields(SKey),
    End = strip_fields(EKey),
    Result = enterdb:read_range(TabName, {Start, End}, Limit),
    make_response(key_columns_list, Result);
apply_procedure({read_range_n, #{table_name := TabName,
				 start_key := StartKey,
				 n := N}}) ->
    Start = strip_fields(StartKey),
    Result = enterdb:read_range_n(TabName, Start, N),
    make_response(key_columns_list, Result);
apply_procedure({batch_write, #{table_name := _TabName,
				delete_keys := _DeleteKeys,
				write_kvps := _WriteKvps}}) ->
    {error, #{cause => {protocol, "function not supported"}}};
apply_procedure({first, #{table_name := TabName}}) ->
    Result = enterdb:first(TabName),
    make_response(kcp_it, Result);
apply_procedure({last, #{table_name := TabName}}) ->
    Result = enterdb:last(TabName),
    make_response(kcp_it, Result);
apply_procedure({seek, #{table_name := TabName,
			 key := Key}}) ->
    Result = enterdb:seek(TabName, strip_fields(Key)),
    make_response(kcp_it, Result);
apply_procedure({next, #{it := It}}) ->
    Result = enterdb:next(It),
    make_response(key_columns_pair, Result);
apply_procedure({prev, #{it := It}}) ->
    Result = enterdb:prev(It),
    make_response(key_columns_pair, Result);
apply_procedure({add_index, #{table_name := TabName,
			     config := Config}}) ->
    IndexConfig = make_index_config(Config),
    Result = enterdb:add_index(TabName, IndexConfig),
    make_response(ok, Result);
apply_procedure({remove_index, #{table_name := TabName,
				 columns := Columns}}) ->
    Result = enterdb:remove_index(TabName, Columns),
    make_response(ok, Result);
apply_procedure({index_read, #{table_name := TabName,
			       column_name := ColumnName,
			       term := Term,
			       filter := Filter}}) ->
    PostingFilter = translate_posting_filter(Filter),
    Result = enterdb:index_read(TabName, ColumnName, Term, PostingFilter),
    make_response(postings, Result);
apply_procedure({list_tables, #{}}) ->
    Result = enterdb:list_tables(),
    make_response(string_list, Result);
apply_procedure(_) ->
    {error, #{cause => {protocol, "unknown procedure"}}}.

-spec send_response(To :: pid,
		    Version :: #{major => pos_integer(),
				 minor => pos_integer()},
		    TransactionId :: integer(),
		    Response :: {atom(), term()}) ->
    ok.

send_response(To, Version, TransactionId, Response) ->
    PDU = #{version => Version,
	    transaction_id => TransactionId,
	    procedure => Response},
    ?debug("Response PDU: ~p", [PDU]),
    Bin = apollo_pb:encode_msg(PDU, 'ApolloPdu'),
    pundun_bp_session:respond(To, Bin).

-spec make_response(Choice :: ok |
			      value |
			      key_columns_pair |
			      key_columns_list |
			      proplist |
			      kcpIt |
			      keys |
			      string_list,
		    Result :: ok |
			      {ok, value()} |
			      {ok, [{atom(), term()}]} |
			      {ok, [kvp()], complete} |
			      {ok, [kvp()], key()} |
			      {ok, [kvp()]} |
			      {ok, kvp(), pid()} |
			      [string()] |
			      {error, term()}) ->
    {response, #{result => tuple(),
		 more_data_to_be_sent => true}} |
    {error, #{cause => tuple()}}.

make_response(ok, ok) ->
    wrap_response({ok, "ok"});
make_response(columns, {ok, Columns}) ->
    wrap_response({columns, make_fields(Columns)});
make_response(key_columns_pair, {ok, {Key, Value}}) ->
    wrap_response({key_columns_pair,
		   #{key => make_seq_of_fields(Key),
		     columns => make_seq_of_fields(Value)}});
make_response(key_columns_list, {ok, KVL, Cont}) ->
    List = [#{key => make_seq_of_fields(K),
	      columns => make_seq_of_fields(V)} || {K,V} <- KVL],
    {Complete, Key} =
	case Cont of
	    complete ->
		{true, []};
	    CKey ->
		{false, make_seq_of_fields(CKey)}
	end,
    Continuation = #{complete => Complete, key => Key},
    KeyColumnsList = #{list => List,
		       continuation => Continuation},
    wrap_response({key_columns_list, KeyColumnsList});
make_response(key_columns_list, {ok, KVL}) ->
    List = [#{key => make_seq_of_fields(K),
	      columns => make_seq_of_fields(V)} || {K, V} <- KVL],
    Continuation = #{complete => true, key => []},
    KeyColumnsList = #{list => List,
		       continuation => Continuation},
    wrap_response({key_columns_list, KeyColumnsList});
make_response(proplist, {ok, List}) ->
    Fields = [#{name => atom_to_list(P),
		value => make_value(A)} || {P, A} <- List],
    Proplist = #{fields => Fields},
    wrap_response({proplist, Proplist});
make_response(kcp_it, {ok, {Key, Value}, Ref}) ->
    Kcp = #{key => make_seq_of_fields(Key),
	    columns => make_seq_of_fields(Value)},
    wrap_response({kcp_it, #{key_columns_pair => Kcp,
			     it => Ref}});
make_response(postings, {ok, Postings}) ->
    List = [#{key => make_seq_of_fields(Key),
	      timestamp => Ts,
	      frequency => Freq,
	      position => Pos} || #{freq := Freq,
				    key := Key,
				    pos := Pos,
				    ts := Ts} <- Postings],
    wrap_response({postings, #{list => List}});
make_response(string_list, List) ->
    FN = #{field_names => List},
    wrap_response({string_list, FN});
make_response(_, {error, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[{error, Reason}])),
    {error, #{cause => {misc, FullStr}}};
make_response(_, {badrpc, Reason}) ->
    FullStr = lists:flatten(io_lib:format("~p",[{badrpc, Reason}])),
    {error, #{cause => {misc, FullStr}}}.

-spec wrap_response(Term :: term()) ->
    {response, #{result => tuple(),
		 more_data_to_be_sent => true | false}}.
wrap_response(Term) ->
    {response, #{result => Term,
		 more_data_to_be_sent => false}}.

-type pbp_table_option() :: {type, type()} |
			    {dataModel, data_model()} |
			    {wrapper, #{}} |
			    {memWrapper, #{}} |
			    {comparator, comparator()} |
			    {timeSeries, boolean()} |
			    {num_of_shards, integer()} |
			    {distributed, boolean()} |
			    {replication_factor, integer()} |
			    {hash_exclude, [string()]}.

-spec make_options(TabOptions :: [#{}]) ->
    [table_option()].
make_options(L) when is_list(L) ->
    Options = [O || #{opt := O} <- L],
    make_options(Options, []).

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
make_options([{hash_exclude, #{field_names := FN}} | Rest], Acc) ->
    make_options(Rest, [{hash_excude, FN} | Acc]);
make_options([_ | Rest], Acc) ->
    make_options(Rest, Acc).

-spec make_fields(Key :: [{string(), term()}]) ->
    #{fields => [Field :: map()]}.
make_fields(Key) when is_list(Key)->
    Fields = [#{name => Name, value => make_value(Value)}
		|| {Name, Value} <- Key],
    #{fields => Fields};
make_fields(Else)->
    ?debug("Invalid key: ~p",[Else]),
    Else.

-spec make_seq_of_fields(Key :: [{string(), term()}]) ->
    [#{}].
make_seq_of_fields(Key) when is_list(Key)->
    [#{name => Name, value => make_value(Value)}
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
    #{type => {binary, V}};
make_value(V) when is_integer(V) ->
    #{type => {int, V}};
make_value(V) when is_float(V) ->
    #{type => {double, V}};
make_value(true) ->
    #{type => {boolean, true}};
make_value(false) ->
    #{type => {boolean, false}};
make_value(L) when is_list(L) ->
    case io_lib:printable_unicode_list(L) of
	true ->
	    #{type => {string, L}};
	false ->
	    #{type => {list, #{values=>[make_value(E) || E <- L]}}}
    end;
make_value(undefined) ->
    #{type => {null, <<>>}};
make_value(A) when is_atom(A) ->
    #{type => {string, atom_to_list(A)}};
make_value(Map) when is_map(Map) ->
    Fun =
	fun(K, V, Acc) when is_list(K) ->
	    Acc#{K => make_value(V)};
	   (K, V, Acc) when is_atom(K) ->
	    Acc#{atom_to_list(K) => make_value(V)}
	end,
    #{type => {map, #{values => maps:fold(Fun, #{}, Map)}}};
make_value(T) when is_tuple(T) ->
    make_value(tuple_to_list(T)).

-spec strip_fields(Fields :: [#{name := string(),
				value := term()}]) ->
    [{string(), term()}].
strip_fields(Fields) ->
    strip_fields(Fields, []).

-spec strip_fields(Fields :: [#{name := string(),
				value := term()}],
		   Acc :: [{string(), term()}]) ->
    [{string(), term()}].
strip_fields([#{value := undefined} = F| Rest], Acc) ->
    ?warning("Unset field received: ~p", [F]),
    strip_fields(Rest, Acc);
strip_fields([#{value := #{type := undefined}} = F | Rest], Acc) ->
    ?warning("Unset field value received: ~p", [F]),
    strip_fields(Rest, Acc);
strip_fields([#{name := N, value := Value} | Rest], Acc) ->
    strip_fields(Rest, [{N, translate_value(Value)} | Acc]);
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
    validate_attributes(T, [index_on | Acc]);
validate_attributes(["index_on" | T], Acc) ->
    validate_attributes(T, [index_on | Acc]);
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

-spec translate_wrapper(#{num_of_buckets := pos_integer(),
			  time_margin := tuple(),
			  size_margin := tuple()} | undefined) ->
    #{} | undefined.
translate_wrapper(#{num_of_buckets := NB,
		    time_margin := TM,
		    size_margin := SM}) ->
    #{num_of_buckets => NB,
      time_margin => TM,
      size_margin => SM};
translate_wrapper(undefined) ->
    undefined.

-spec translate_tda(#{num_of_buckets := pos_integer(),
		      time_margin  := tuple(),
		      ts_field := string(),
		      precision := atom()} | undefined) ->
    #{} | undefined.
translate_tda(#{num_of_buckets := NB,
		time_margin := TM,
		ts_field := TsF,
		precision := Precision}) ->
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

-spec translate_update_operation(UpdateOperation ::[#{field := string(),
						      update_instruction := #{},
						      value := #{},
						      default_value  := #{}}]) ->
    term().
translate_update_operation(UpdateOperation) ->
    translate_update_operation(UpdateOperation, []).

-spec translate_update_operation(UpdateOperation ::[#{field := string(),
						      update_instruction := #{},
						      value := #{},
						      default_value  := #{}}],
				 Acc :: term()) ->
    term().
translate_update_operation([#{field := F,
			      update_instruction := UpInst,
			      value := Value,
			      default_value := DefaultValue} | Rest], Acc) ->
    I = translate_update_instruction(UpInst),
    V = translate_value(Value),
    InitList =
	case DefaultValue of
	    #{} ->
		DV = translate_value(DefaultValue),
		[{1,F}, {2, I}, {3, V}, {4, DV}];
	    undefined ->
		[{1,F}, {2, I}, {3, V}]
	end,
    Tuple = erlang:make_tuple(length(InitList), undefined, InitList),
    translate_update_operation(Rest, [Tuple | Acc]);
translate_update_operation([], Acc) ->
    lists:reverse(Acc).

-spec translate_value(#{type := tuple()} | undefined) ->
    term().
translate_value(#{type := {boolean, Term}}) ->
    Term;
translate_value(#{type := {int, Term}}) ->
    Term;
translate_value(#{type := {binary, Term}}) ->
    Term;
translate_value(#{type := {null, _}}) ->
    undefined;
translate_value(#{type := {double, Term}}) ->
    Term;
translate_value(#{type := {string, Term}}) ->
    Term;
translate_value(#{type := {list, #{values := Values}}}) ->
    [translate_value(V) || #{type := Tuple} = V <- Values, Tuple =/= undefined];
translate_value(#{type := {map, #{values := Values}}}) ->
    Fun =
	fun(K, #{type := T} = V, Acc) when is_list(K), is_tuple(T) ->
	    Acc#{K => translate_value(V)}
	end,
    maps:fold(Fun, #{}, Values).

-spec translate_update_instruction(UpInst :: #{instruction := 'INCREMENT' | 'OVERWRITE',
					       threshold := binary(),
					       set_value := binary()}) ->
    term().
translate_update_instruction(#{instruction := 'INCREMENT',
			       threshold := <<>>,
			       set_value := <<>>}) ->
    increment;
translate_update_instruction(#{instruction := 'INCREMENT',
			       threshold := Threshold,
			       set_value := SetValue}) ->
    {increment,
     decode_unsigned_default(Threshold, undefined),
     decode_unsigned_default(SetValue, undefined)};
translate_update_instruction(#{instruction := 'OVERWRITE'}) ->
    overwrite.

make_index_config(List) ->
    make_index_config(List, []).

make_index_config([#{column := C, options := Opts} | Rest], Acc) ->
    make_index_config(Rest, [{C, make_index_options(Opts)} | Acc]);
make_index_config([], Acc) ->
    lists:reverse(Acc).

make_index_options(#{char_filter := CF,
		     tokenizer := Tokenizer,
		     token_filter := TokenFilter}) ->
    M1 = translate_char_filter(CF, #{}),
    M2 = translate_tokeizer(Tokenizer, M1),
    make_token_filter(TokenFilter, M2);
make_index_options(undefined) ->
    undefined.

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

make_token_filter(#{transform := Transform,
		    add := Add,
		    delete := Delete,
		    stats := Stats}, M) ->
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
make_add_words(undefined, M)->
    M;
make_add_words(Add, M)->
    M#{add => Add}.

make_delete_words([], M) ->
    M;
make_delete_words(undefined, M) ->
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
    M#{stats => positions};
translate_stats('NOSTATS', M) ->
    M.

translate_posting_filter(undefined) ->
    undefined;
translate_posting_filter(#{sort_by := 'RELEVANCE',
			   start_ts := <<>>,
			   end_ts := <<>>,
			   max_postings := 0}) ->
    undefined;
translate_posting_filter(#{sort_by := SortBy,
			   start_ts := StartTs,
			   end_ts := EndTs,
			   max_postings := MaxPostings}) ->
    M1 = translate_sort_by(SortBy, #{}),
    M2 = set_value(start_ts, decode_unsigned_default(StartTs, undefined), M1),
    M3 = set_value(end_ts, decode_unsigned_default(EndTs, undefined), M2),
    set_value(max_postings, MaxPostings, M3).

decode_unsigned_default(<<>>, Default) ->
    Default;
decode_unsigned_default(Bin, _) when is_binary(Bin) ->
    binary:decode_unsigned(Bin, big);
decode_unsigned_default(_, Default) ->
    Default.

translate_sort_by('RELEVANCE', M) ->
    M#{sort_by => relevance};
translate_sort_by('TIMESTAMP', M) ->
    M#{sort_by => timestamp};
translate_sort_by(_, M) ->
    M.

set_value(_Attr, undefined, Map) ->
    Map;
set_value(Attr, Val, Map) ->
    Map#{Attr => Val}.
