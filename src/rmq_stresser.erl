-module(rmq_stresser).

-include_lib("amqp_client/include/amqp_client.hrl").

%% API exports
-export([main/1, run/1, stop/1, run_many/2, stop_many/1]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    io:format("Args: ~p~n", [Args]),
    erlang:halt(0).


run({RepoName, User, Pass}) ->
    Params = #amqp_params_network {
        username = User,
        password = Pass,
        virtual_host = <<"/cvmfs">>,
        host = "cvmfs-notify-01.cern.ch",
        port = 5672,
        channel_max = 2047,
        frame_max = 0,
        heartbeat = 30
    },

    {ok, Connection} = amqp_connection:start(Params),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, #'queue.declare'{exclusive=true}),

    Binding = #'queue.bind'{
        queue       = Queue,
        exchange    = <<"repository_activity">>,
        routing_key = RepoName
    },
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),

    Pid = spawn(fun() -> loop(Channel, 1) end),

    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = _Tag} = amqp_channel:subscribe(Channel, Sub, Pid),

    {Channel, Connection}.


run_many(Creds, N) ->
    Hds = run_many_helper(Creds, 0, N, []),
    io:format("Started ~p consumer processes.~n", [length(Hds)]),
    Hds.


stop({Channel, Connection}) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.


stop_many(Handles) ->
    lists:foreach(fun(Hd) -> stop(Hd) end, Handles).


%%====================================================================
%% Internal functions
%%====================================================================

loop(Channel, Idx) ->
    receive
        %% This is the first message received
        #'basic.consume_ok'{} ->
            loop(Channel, Idx);

        %% This is received when the subscription is cancelled
        #'basic.cancel_ok'{} ->
            ok;

        %% A delivery
        {#'basic.deliver'{delivery_tag = Tag}, _Content} ->
            %% Do something with the message payload
            %% (some work here)
            %io:format("~p received message (#~p)~n", [self(), Idx]),
            %%io:format("Message received: ~p", [Content]),

            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

            %% Loop
            loop(Channel, Idx + 1)
    end.

run_many_helper(Creds, Idx, N, Hds) when Idx < N ->
    Hd = run(Creds),
    %timer:sleep(10),
    run_many_helper(Creds, Idx + 1, N, [Hd | Hds]);
run_many_helper(_Creds, Idx, N, Hds) when Idx == N ->
    Hds.

