%%%-------------------------------------------------------------------
%%% File    : example_SUITE.erl
%%% Author  :
%%% Description :
%%%
%%% Created :
%%%-------------------------------------------------------------------
-module(pundun_api_test_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
%%-include("gb_hash.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{minutes,10}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    erlang:set_cookie(node(), pundun),
    {ok, Hostname} = inet:gethostname(),
    [{node, list_to_atom("pundun@" ++ Hostname)}| Config].

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------
groups() ->
    [].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%% Reason = term()
%%   The reason for skipping all groups and test cases.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%--------------------------------------------------------------------
all() ->
    [create_leveldb_table,
     write_to_leveldb_table,
     read_from_leveldb_table,
     delete_from_leveldb_table,
     read_range_from_leveldb_table,
     read_range_n_from_leveldb_table,
     iterate_over_leveldb_table,
     delete_leveldb_table].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: TestCase() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Test case info function - returns list of tuples to set
%%              properties for the test case.
%%
%% Note: This function is only meant to be used to return a list of
%% values, not perform any other operations.
%%--------------------------------------------------------------------
create_leveldb_table() ->
    [].

delete_leveldb_table() ->
    [].

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%% Comment = term()
%%   A comment about the test case that will be printed in the html log.
%%
%% Description: Test case function. (The name of it must be specified in
%%              the all/0 list or in a test case group for the test case
%%              to be executed).
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Create a hash ring with SHA algorithm and uniform strategy.
%% @end
%%--------------------------------------------------------------------
create_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Create table at: ~p", [Node])),
    Name = "ct_test_101",
    KeyDef = [id, ts],
    ColumnsDef = [field1],
    IndexesDef = [],
    Options = [{time_ordered, true},
	       {type, leveldb},
	       {data_model, binary},
	       {shards, 8},
	       {nodes, [Node]}],
    Args = [Name, KeyDef, ColumnsDef, IndexesDef, Options],
    ?assert(ok == rpc:call(Node, enterdb, create_table, Args)).

write_to_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Write to table at: ~p", [Node])),

    Name = "ct_test_101",
    Key = [{ts, {0,0,1}}, {id, 1}],
    Columns = [{field1, 1}],
    Args = [Name, Key, Columns],
    Res = rpc:call(Node, enterdb, write, Args),
    ct:log(io_lib:format("RPC Result: ~p", [Res])),
    ?assert(ok == Res).

read_from_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Read from table at: ~p", [Node])),

    Name = "ct_test_101",
    Key = [{id, 1}, {ts, {0,0,1}}],
    Columns = [{field1, 1}],
    Args = [Name, Key],

    Res = rpc:call(Node, enterdb, read, Args),
    ct:log(io_lib:format("RPC Result: ~p", [Res])),
    ?assert({ok, Columns} == Res).

delete_from_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Delete from table at: ~p", [Node])),

    Name = "ct_test_101",
    Key = [{id, 1}, {ts, {0,0,1}}],
    Args = [Name, Key],

    DelRes = rpc:call(Node, enterdb, delete, Args),
    ct:log(io_lib:format("RPC Result for delete: ~p", [DelRes])),
    ?assert(ok == DelRes),
    ReadRes = rpc:call(Node, enterdb, read, Args),
    ct:log(io_lib:format("RPC Result for read: ~p", [ReadRes])),
    ?assertMatch({error, _}, ReadRes).

read_range_from_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Read Range from table at: ~p", [Node])),

    Name = "ct_test_101",
    Items = lists:seq(1, 1000),
    WriteResults = [ rpc:call(Node, enterdb, write,
			      [Name, [{id, X}, {ts, {0,0,X}}], [{field1, X}] ])
				|| X <- Items ],

    ?assert([ok] == lists:usort(WriteResults)),

    Range = {[{ts, {0,0,999}}, {id, 999}] , [{id, 5}, {ts, {0,0,5}}]},
    Chunk = 1000,
    ReadArgs = [Name, Range, Chunk],

    ReadRes = rpc:call(Node, enterdb, read_range, ReadArgs),
    ?assertMatch({ok, _, complete}, ReadRes),
    ?assert(995 == length(element(2, ReadRes))).

read_range_n_from_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Read Range N from table at: ~p", [Node])),

    Name = "ct_test_101",

    Start = [{ts, {0,0,0}}, {id, 1100}],
    N = 1100,
    ReadArgs = [Name, Start, N],

    ReadRes = rpc:call(Node, enterdb, read_range_n, ReadArgs),
    ?assertMatch({ok, _}, ReadRes),
    ?assert(1000 == length(element(2, ReadRes))).

iterate_over_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Iterate over table at: ~p", [Node])),

    Name = "ct_test_101",
    {_,_,Ref} = Result = rpc:call(Node, enterdb, first, [Name]),
    ct:log(io_lib:format("first reult: ~p", [Result])),
    ?assertMatch({ok, {[{id, 1000}, {ts, {0,0,1000}}], [{field1, 1000}]}, _}, Result),
    ?assertMatch({ok, {_,_}} , rpc:call(Node, enterdb, next, [Ref])),
    ?assertMatch({ok, {_,_}} , rpc:call(Node, enterdb, next, [Ref])),
    ?assertMatch({ok, {_,_}} , rpc:call(Node, enterdb, prev, [Ref])),
    ?assertMatch({ok, {_,_}} , rpc:call(Node, enterdb, prev, [Ref])),
    ?assertMatch({error, invalid} , rpc:call(Node, enterdb, prev, [Ref])).



delete_leveldb_table(Config) ->
    Node = proplists:get_value(node, Config),
    ct:log(io_lib:format("Delete table at: ~p", [Node])),
    Name = "ct_test_101",
    ?assert(ok == rpc:call(Node, enterdb, delete_table, [Name])).
