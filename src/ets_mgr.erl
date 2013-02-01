%%% @author Alex Arnell <alex@bravenet.com>
%%% @copyright 2013 Bravenet Media
%%% @doc
%%% An ets table manager.
%%%
%%% This simple server process manages any ets tables. It will declare itself
%%% as the hier and return ownership to the calling process.
%%% @end

-module(ets_mgr).
-behaviour(gen_server).

%% Public API
-export([start_link/0, new/2, file2tab/1, file2tab/2, give_away/1, watch/1, recover/2]).

%% OTP gen_server Callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-export([ping/0]).
-endif.

%%%-------------------------------------------------------------------
%%% Types & Macros
%%%-------------------------------------------------------------------

-record(state, {
        orphaned=dict:new() % dictionary of orphaned ets tables
        }).

%%%-------------------------------------------------------------------
%%% Public API
%%%-------------------------------------------------------------------

%% @doc
%% Starts the server.
%% @end
-spec start_link() -> Reply when
    Reply :: {ok, pid()} |
             ignore |
             {error, Reason},
    Reason :: term().

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc
%% Creates a new ets table.
%%
%% The options accepted are identical to those found in the documentation for
%% ets:new/2, however this function will overwrite any provided heir options.
%% @end
-spec new(Name, Options) -> ets:tid() | atom() when
    Name :: atom() | {atom(), term()},
    Options :: [Option],
    Option :: Type | Access | named_table | {keypos, Pos} | Tweaks,
    Type :: set | ordered_set | bag | duplicate_bag,
    Access :: public | protected | private,
    Tweaks :: {write_concurrency, boolean()} | {read_concurrency, boolean()} | compressed,
    Pos :: integer().

new(Name, Options) when is_atom(Name) ->
    case lists:member(named_table, Options) of
        true ->
            % ref will be ignored, named tables are unique enough already
            new({Name, make_ref()}, Options);
        false ->
            error(not_unique, [Name, Options])
    end;

new(Name, Options) when is_tuple(Name) ->
    case gen_server:call(?MODULE, {new, [Name, Options]}) of
        {trapped, Class, Reason} ->
            erlang:Class(Reason);
        Reply ->
            Reply
    end.

%% @doc
%% Reads a file produced from ets:tab2file/2 or ets:tab2file/3 and creates the corresponding table Tab.
%%
%% See ets:file2tab/1 for full details.
%% @end
-spec file2tab(Filename) -> {ok, Tab} | {error, Reason} when
    Filename :: file:name(),
    Tab :: ets:tab(),
    Reason :: term().

file2tab(Filename) ->
    file2tab(Filename, []).

%% @doc
%% Reads a file produced from ets:tab2file/2 or ets:tab2file/3 and creates the corresponding table Tab.
%%
%% See ets:file2tab/1 for full details.
%% @end
-spec file2tab(Filename, Options) -> {ok, Tab} | {error, Reason} when
    Filename :: file:name(),
    Options :: [Option],
    Option :: {verify, boolean()},
    Tab :: ets:tab(),
    Reason :: term().

file2tab(Filename, Options) ->
    gen_server:call(?MODULE, {file2tab, Filename, Options}).

%% @doc
%% Returns ownership of an orphaned ets table to calling process.
%% @end
-spec give_away(Name) -> ets:tid() | not_found when
    Name :: atom() | {atom(), term()}.

give_away(Name) ->
    gen_server:call(?MODULE, {give_away, Name}).

watch(Tab) when is_atom(Tab) ->
    ets:setopts(Tab, {heir, whereis(?MODULE), Tab}).

%% @doc
%% Used to recover an ETS table first from the manager and then optionally from disk.
%%
%% To recover an ETS table from disk recover/2 expects a {filename, Filename} option
%% to be present in the Opts list. Filename should be the file to read from disk.
%%
%% Opts contain any options required to construct the new ETS table as well as any
%% options required to recover from disk. This makes it possible to verify a disk
%% loaded table.
%% @end
-spec recover(Name, Opts) -> {ok, Tab} | {error, Reason} when
    Name :: atom(),
    Opts :: [Opt],
    Tab :: ets:tab(),
    Reason :: term(),
    Opt :: {filename, file:name()} | term(). % see ets:new/2 and ets:file2tab/2

recover(Name, Opts) ->
    case ?MODULE:give_away(Name) of
        not_found ->
            case recover_from_disk(Opts) of
                not_found ->
                    SafeOpts = ets_new_from_recover_options(Opts),
                    ?MODULE:new(Name, SafeOpts);
                Response -> Response
            end;
        Tab -> {ok, Tab}
    end.

-ifdef(TEST).

%% @doc
%% Retuns a pong, useful for testing.
%% @end
-spec ping() -> pong.

ping() ->
    gen_server:call(?MODULE, ping).

-endif.

%%%-------------------------------------------------------------------
%%% OTP gen_server callbacks
%%%-------------------------------------------------------------------

%% @private
%% @doc
%% Initializes the server.
%% @end
-spec init(Args) -> Result when
    Args :: term(),
    Result :: {ok, State} | {ok, State, Timeout} |
              {ok, State, hibernate} |
              ignore | {stop, Reason},
    State :: term(),
    Timeout :: pos_integer() | infinity,
    Reason :: term().

init([]) ->
    % make sure we get notified of shutdowns
    process_flag(trap_exit, true),

    {ok, #state{}}.

%% @private
%% @doc
%% Handling call messages.
%% @end
-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(), Tag},
    Tag :: term(),
    State :: term(),
    Result :: {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} |
              {noreply, NewState} | {noreply, NewState, Timeout} |
              {noreply, NewState, hibernate} |
              {stop, Reason, Reply, NewState} | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: pos_integer() | infinity,
    Reason :: term().

handle_call({new, [{Name, Ref}, DirtyOpts]}, {Owner, _}, State0) ->
    try
        % strip out any existing heir data
        % TODO maybe look at appending to HeirData
        Options = remove_heir_options(DirtyOpts),

        % needs uniquely named tables to ensure give_away behavior
        HeirData = case lists:member(named_table, Options) of
            true ->
                % named tables are already unique enough
                Name;
            false ->
                {Name, Ref}
        end,

        % construct the table assigning self as the heir
        Tab = ets:new(Name, [{heir, self(), HeirData} | Options]),

        % give the table back to the caller
        ets:give_away(Tab, Owner, []),

        % and reply with the table id
        {reply, Tab, State0}
    catch
        Class:Pattern ->
            {reply, {trapped, Class, Pattern}, State0}
    end;

handle_call({file2tab, Filename, Options}, {Owner, _}, State0) ->
    case ets:file2tab(Filename, Options) of
        {error, _} = Error ->
            {reply, Error, State0};
        {ok, Tab} = Reply ->
            HeirData = case ets:info(Tab, named_table) of
                true -> Tab;
                false -> {Tab, make_ref()}
            end,

            ets:setopts(Tab, {heir, self(), HeirData}),
            ets:give_away(Tab, Owner, []),

            {reply, Reply, State0}
    end;

handle_call({give_away, Name}, {Owner, _}, State0) ->
    case del_orphan(Name, State0) of
        not_found ->
            {reply, not_found, State0};
        {Tab, State1} ->
            ets:give_away(Tab, Owner, []),
            {reply, Tab, State1}
    end;

handle_call(ping, _From, State) ->
    {reply, pong, State};

handle_call(_Request, _From, State) ->
    Reply = not_implemented,
    {reply, Reply, State}.

%% @private
%% @doc
%% Handling cast messages.
%% @end
-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: term(),
    Result :: {noreply, NewState} | {noreply, NewState, Timeout} |
              {noreply, NewState, hibernate} |
              {stop, Reason, NewState},
    NewState :: term(),
    Timeout :: pos_integer() | infinity,
    Reason :: term().

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc
%% Handling all non call/cast messages.
%% @end
-spec handle_info(Info, State) -> Result when
    Info :: term(),
    State :: term(),
    Result :: {noreply, NewState} | {noreply, NewState, Timeout} |
              {noreply, NewState, hibernate} |
              {stop, Reason, NewState},
    NewState :: term(),
    Timeout :: pos_integer() | infinity,
    Reason :: term().

handle_info({'EXIT', _Pid, _Reason}, State) ->
    % handle trapped exit signals from linked processes
    {noreply, State};

handle_info({'ETS-TRANSFER', Tab, _FromPid, HeirData}, State0) ->
    % handle ets table transfers from other processes
    {noreply, add_orphan(HeirData, Tab, State0)};

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
-spec terminate(Reason, State) -> no_return() when
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: term().

terminate(shutdown, _State) ->
    % handle being ordered to shutdown from supervisor
    ok;

terminate(_Reason, _State) ->
    ok.

%% @private
%% @doc
%% Convert process state when code is changed
%% @end
-spec code_change(OldVsn, State, Extra) -> Result when
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term(),
    State :: term(),
    Extra :: term(),
    Result :: {ok, NewState} | {error, Reason},
    NewState :: term(),
    Reason :: term().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%-------------------------------------------------------------------
%%% Internal Functions
%%%-------------------------------------------------------------------

-spec recover_from_disk(Opts) -> not_found | {ok, Tab} | {error, Reason} when
    Opts :: [Option],
    Option :: {filename, file:name()} | {verify, boolean()},
    Tab :: ets:tab(),
    Reason :: term().

recover_from_disk(Opts) ->
    case lists:keyfind(filename, 1, Opts) of
        false ->
            not_found;
        {filename, Filename} ->
            SafeOpts = case lists:keyfind(verify, 1, Opts) of
                false -> [];
                VerifyOpt -> [VerifyOpt]
            end,
            case ?MODULE:file2tab(Filename, SafeOpts) of
                % not sure if erlang will mutate iolists not not, hence _Filename
                {error, {read_error, {file_error, _Filename, enoent}}} ->
                    not_found;
                Response -> Response
            end
    end.

-spec ets_new_from_recover_options(Opts) -> SafeOpts when
    Opts :: [term()],
    SafeOpts :: [term()].

ets_new_from_recover_options(Opts) ->
    lists:keydelete(verify, 1, lists:keydelete(filename, 1, Opts)).

remove_heir_options(Options) ->
    lists:dropwhile(
        fun (Elem) when is_tuple(Elem), element(1, Elem) =:= heir ->
                true;
            (_) ->
                false
        end,
        Options).

add_orphan(Key, Tab, State0) ->
    State0#state{ orphaned=dict:store(Key, Tab, State0#state.orphaned) }.

del_orphan(Key, #state{ orphaned=Orphans }=State0) ->
    case dict:find(Key, Orphans) of
        {ok, Tab} ->
            {Tab, State0#state{ orphaned=dict:erase(Key, Orphans) }};
        error ->
            not_found
    end.
