%%% @author Alex Arnell <alex@bravenet.com>
%%% @copyright 2012 Bravenet Media
%%% @doc
%%% Test Suite for ets_mgr.
%%% @end

-module(ets_mgr_tests).
-vsn(0).

-include_lib("eunit/include/eunit.hrl").

%%%-------------------------------------------------------------------
%%% Test Generators
%%%-------------------------------------------------------------------

suite_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [fun create_anon_tables/1,
      fun strips_existing_heir/1,
      fun create_named_tables/1,
      fun reads_tables_from_file/1,
      fun recovers_tables/1,
      fun gives_away_tables/1,
      fun does_not_crash/1,
      fun provides_recover_function/1]
    }.

%%%-------------------------------------------------------------------
%%% Fixtures
%%%-------------------------------------------------------------------

setup() ->
    {ok, Pid} = ets_mgr:start_link(),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            % unlink the process so we don't crash ourselves
            unlink(Pid),
            % send a graceful shutdown message
            exit(Pid, shutdown),
            wait_for_shutdown(Pid);
        false ->
            ignore
    end.

%%%-------------------------------------------------------------------
%%% Tests
%%%-------------------------------------------------------------------

create_anon_tables(Pid) ->
    fun () ->
            Self = self(),
            Tab = ets_mgr:new({test_name, uid1}, []),

            ?assertEqual(test_name, ets:info(Tab, name)),
            ?assertEqual(Self, ets:info(Tab, owner)),
            ?assertEqual(Pid, ets:info(Tab, heir))
    end.

strips_existing_heir(Pid) ->
    fun () ->
            Tab = ets_mgr:new({test_name, uid1}, [{heir, self(), foobar}]),

            ?assertEqual(self(), ets:info(Tab, owner)),
            ?assertEqual(Pid, ets:info(Tab, heir))
    end.

create_named_tables(Pid) ->
    fun () ->
            Self = self(),
            ?assertEqual(test_table, ets_mgr:new(test_table, [named_table])),

            ?assertEqual(Self, ets:info(test_table, owner)),
            ?assertEqual(Pid, ets:info(test_table, heir))
    end.

recovers_tables(Pid) ->
    {Owner, Tab} = spawn_test_process({test_table, make_ref()}, []),
    Tab = call(Owner, tab),
    {inorder,
     [?_assertEqual(Owner, ets:info(Tab, owner)),
      ?_assertEqual(Pid, ets:info(Tab, heir)),
      ?_assertEqual(stopped, call(Owner, stop)),
      ?_assert(wait_for_shutdown(Owner)),
      ?_assertEqual(Pid, ets:info(Tab, owner))]}.

reads_tables_from_file(EtsMgrPid) ->
    {foreach,
     fun () ->
                TabFilename = "test_table.ets",
                NewOwner = spawn_file2tab_process(),
                [{heir, EtsMgrPid}, {filename, TabFilename}, {new_owner, NewOwner}]
        end,
     fun teardown_table_test/1,
     [fun named_table_file_reads/1,
      fun unnamed_table_file_reads/1,
      fun table_file_reads_with_options/1]}.

teardown_table_test(Props) ->
    case proplists:is_defined(new_owner, Props) of
        false -> ok;
        true ->
            NewOwner = proplists:get_value(new_owner, Props),
            case is_process_alive(NewOwner) of
                false -> ok;
                true ->
                    call(NewOwner, stop),
                    wait_for_shutdown(NewOwner)
            end
    end,

    case proplists:is_defined(filename, Props) of
        false -> ok;
        true ->
            file:delete(proplists:get_value(filename, Props))
    end.

-define(get(Key), (proplists:get_value(Key, Props))).

named_table_file_reads(Props) ->
    Heir = ?get(heir),
    {Owner, Tab} = spawn_test_process(test_table, [named_table]),
    Tab = call(Owner, tab),
    TabFilename = ?get(filename),
    NewOwner = ?get(new_owner),
    {inorder,
     [?_assertEqual(Owner, ets:info(Tab, owner)),
      ?_assertEqual(Heir, ets:info(Tab, heir)),
      ?_assertEqual(ok, ets:tab2file(Tab, TabFilename)),
      ?_assertEqual(true, call(Owner, delete)),
      ?_assertEqual(stopped, call(Owner, stop)),
      ?_assert(wait_for_shutdown(Owner)),
      ?_assertEqual({ok, Tab}, call(NewOwner, {file2tab, TabFilename})),
      ?_assertEqual(NewOwner, ets:info(Tab, owner)),
      ?_assertEqual(Heir, ets:info(Tab, heir)),
      % not really tests, just cleaning up
      ?_assertEqual(stopped, call(NewOwner, stop)),
      ?_assertEqual(ok, file:delete(TabFilename))]}.

unnamed_table_file_reads(Props) ->
    fun () ->
            {Owner, Tab} = spawn_test_process({test_table, make_ref()}, []),
            Tab = call(Owner, tab),
            Heir = ?get(heir),
            TabFilename = ?get(filename),
            NewOwner = ?get(new_owner),

            ?assertEqual(Owner, ets:info(Tab, owner)),
            ?assertEqual(Heir, ets:info(Tab, heir)),
            ?assertEqual(ok, ets:tab2file(Tab, TabFilename)),
            ?assertEqual(true, call(Owner, delete)),
            ?assertEqual(stopped, call(Owner, stop)),
            ?assert(wait_for_shutdown(Owner)),

            Response = call(NewOwner, {file2tab, TabFilename}),
            ?assertMatch({ok, _}, Response),
            {ok, NewTabId} = Response,

            ?assertEqual(NewOwner, ets:info(NewTabId, owner)),
            ?assertEqual(Heir, ets:info(NewTabId, heir))
    end.

table_file_reads_with_options(Props) ->
    fun () ->
            {Owner, Tab} = spawn_test_process({test_table, make_ref()}, []),
            Tab = call(Owner, tab),
            Heir = ?get(heir),
            TabFilename = ?get(filename),
            NewOwner = ?get(new_owner),

            ?assertEqual(Owner, ets:info(Tab, owner)),
            ?assertEqual(Heir, ets:info(Tab, heir)),

            ?assertEqual(ok, ets:tab2file(Tab, TabFilename, [{extended_info, [object_count, md5sum]}])),
            ?assertEqual(true, call(Owner, delete)),

            Response = call(NewOwner, {file2tab, TabFilename, [{verify, true}]}),
            ?assertMatch({ok, _}, Response),
            {ok, NewTabId} = Response,

            ?assertEqual(NewOwner, ets:info(NewTabId, owner)),
            ?assertEqual(Heir, ets:info(NewTabId, heir)),

            ?assertEqual(stopped, call(Owner, stop)),
            ?assert(wait_for_shutdown(Owner))
    end.

provides_recover_function(EtsMgrPid) ->
    {foreach,
     fun () ->
                TabFilename = "test_table.ets",
                [{heir, EtsMgrPid}, {filename, TabFilename}]
        end,
     fun teardown_table_test/1,
     [fun recovers_using_give_away/1,
      fun recovers_using_file2tab/1,
      fun recovers_using_new/1]}.

recovers_using_give_away(_Props) ->
    fun () ->
            meck:new(ets_mgr, [passthrough]),
            meck:expect(ets_mgr, give_away, 1, test_table),

            ?assertEqual({ok, test_table}, ets_mgr:recover(test_table, [])),

            ?assert(meck:called(ets_mgr, give_away, [test_table])),
            ?assertNot(meck:called(ets_mgr, file2tab, ["test_table.ets", []])),
            ?assertNot(meck:called(ets_mgr, new, [test_table, []])),

            meck:unload(ets_mgr)
    end.

recovers_using_file2tab(_Props) ->
    fun () ->
            meck:new(ets_mgr, [passthrough]),
            meck:expect(ets_mgr, give_away, 1, not_found),
            meck:expect(ets_mgr, file2tab, 2, {ok, test_table}),

            ets_mgr:recover(test_table, [{filename, "test_table.ets"}]),

            ?assert(meck:called(ets_mgr, give_away, [test_table])),
            ?assert(meck:called(ets_mgr, file2tab, ["test_table.ets", []])),
            ?assertNot(meck:called(ets_mgr, new, [test_table, []])),

            meck:unload(ets_mgr)
    end.

recovers_using_new(_Props) ->
    fun () ->
            meck:new(ets_mgr, [passthrough]),
            meck:expect(ets_mgr, give_away, 1, not_found),
            meck:expect(ets_mgr, file2tab, 2, not_found),
            meck:expect(ets_mgr, new, 2, test_table),

            ets_mgr:recover(test_table, [{filename, "test_table.ets"}, ordered_set, public]),

            ?assert(meck:called(ets_mgr, give_away, [test_table])),
            ?assert(meck:called(ets_mgr, file2tab, ["test_table.ets", []])),
            ?assert(meck:called(ets_mgr, new, [test_table, [ordered_set, public]])),

            meck:unload(ets_mgr)
    end.

gives_away_tables(Pid) ->
    TabId = {test_table, make_ref()},
    {Owner, Tab} = spawn_test_process(TabId, []),
    NewOwner = spawn_reclaimer(),
    {inorder,
     [?_assertEqual(Owner, ets:info(Tab, owner)),
      ?_assertEqual(Pid, ets:info(Tab, heir)),
      ?_assertEqual(stopped, call(Owner, stop)),
      ?_assert(wait_for_shutdown(Owner)),
      ?_assertEqual(Pid, ets:info(Tab, owner)),
      ?_assertEqual(Tab, call(NewOwner, {claim, TabId})),
      ?_assertEqual(NewOwner, ets:info(Tab, owner)),
      ?_assertEqual(Pid, ets:info(Tab, heir))]}.

does_not_crash(Pid) ->
    {inorder,
     [?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % random messages
      ?_test(Pid ! random_msg),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % random gen_server calls
      ?_assertEqual(not_implemented, gen_server:call(Pid, random_msg)),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % random gen_server casts
      ?_assertEqual(ok, gen_server:cast(Pid, random_msg)),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % attempt ets construction with bad args
      ?_assertError(badarg, ets_mgr:new({table, id}, [foo])),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % attempt non_unique ets construction
      ?_assertError(not_unique, ets_mgr:new(table, [])),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % attempt duplicate named_table ets construction
      ?_assertEqual(duplicate_name, ets_mgr:new(duplicate_name, [named_table])),
      ?_assertError(badarg, ets_mgr:new(duplicate_name, [named_table])),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % attempt to give_away non-existing table
      ?_assertEqual(not_found, ets_mgr:give_away({does_not_exist, id})),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      % attempt to read non-existing file
      ?_assertMatch({error, _}, ets_mgr:file2tab("should_not_exists.ets")),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr)),

      ?_assertMatch({error, _}, ets_mgr:file2tab("should_not_exists.ets", [{bad_option, false}])),
      ?_assertEqual(pong, ets_mgr:ping()),
      ?_assertEqual(Pid, whereis(ets_mgr))]}.

%%%-------------------------------------------------------------------
%%% Tests
%%%-------------------------------------------------------------------

wait_for_shutdown(Pid) ->
    case is_process_alive(Pid) of
        true ->
            timer:sleep(1),
            wait_for_shutdown(Pid);
        false ->
            true
    end.

call(Pid, Msg) ->
    Pid ! {Msg, self()},
    receive
        Reply -> Reply
    after
        5000 -> error(reply_not_rcvd)
    end.

spawn_test_process(Name, Options) ->
    Pid = spawn(fun () ->
                Tab = ets_mgr:new(Name, Options),
                test_process_loop(Tab)
        end),
    {Pid, call(Pid, tab)}.

test_process_loop(Tab) ->
    receive
        {tab, From} ->
            From ! Tab,
            test_process_loop(Tab);
        {delete, From} ->
            From ! ets:delete(Tab),
            test_process_loop(Tab);
        {stop, From} ->
            From ! stopped
    after
        5000 ->
            test_process_loop(Tab)
    end.

spawn_file2tab_process() ->
    spawn(fun file2tab_loop/0).

file2tab_loop() ->
    receive
        {{file2tab, TabId}, From} ->
            Responses = ets_mgr:file2tab(TabId),
            From ! Responses,
            file2tab_loop();
        {{file2tab, TabId, Options}, From} ->
            Responses = ets_mgr:file2tab(TabId, Options),
            From ! Responses,
            file2tab_loop();
        {stop, From} ->
            From ! stopped
    end.

spawn_reclaimer() ->
    spawn(fun reclaimer_loop/0).

reclaimer_loop() ->
    receive
        {{claim, TabId}, From} ->
            Tab = ets_mgr:give_away(TabId),
            From ! Tab,
            reclaimer_loop();
        {stop, From} ->
            From ! stopped
    after
        5000 -> reclaimer_loop()
    end.

