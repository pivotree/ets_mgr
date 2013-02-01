%%% @author Alex Arnell <alex@bravenet.com>
%%% @copyright 2013 Bravenet Media
%%% @doc
%%% ETS Manager OTP Supervisor
%%% @end
-module(ets_mgr_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%%%-------------------------------------------------------------------
%%% Public API
%%%-------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%-------------------------------------------------------------------
%%% OTP superviser callbacks
%%%-------------------------------------------------------------------

init([]) ->
    {ok, { {one_for_one, 5, 10}, [?CHILD(ets_mgr, worker)]} }.

