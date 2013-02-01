%%% @author Alex Arnell <alex@bravenet.com>
%%% @copyright 2013 Bravenet Media
%%% @doc
%%% ETS Manager OTP Application
%%% @end
-module(ets_mgr_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    ets_mgr_sup:start_link().

stop(_State) ->
    ok.
