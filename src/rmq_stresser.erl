-module(rmq_stresser).

-include_lib("amqp_client/include/amqp_client.hrl").

%% API exports
-export([main/1, run/2, stop/1, run_many/2, stop_many/1]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    io:format("Args: ~p~n", [Args]),
    erlang:halt(0).


run({RepoName, User, Pass}, Parent) ->
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

    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Sub, self()),

    Parent ! {Tag, Channel, Connection},

    subscription_loop(Parent, Channel, 0).


run_many(Creds, N) ->
    T0 = erlang:monotonic_time(millisecond),
    run_many_helper(Creds, self(), 0, N),
    Hds = startup_loop([], N),
    T1 = erlang:monotonic_time(millisecond),
    io:format("Started ~p consumer processes in ~pms.~n", [length(Hds), T1 - T0]),
    Hds.


stop({Tag, Channel, Connection}) ->
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.


stop_many(Handles) ->
    lists:foreach(fun(Hd) -> spawn(fun() -> stop(Hd) end), timer:sleep(1) end, Handles),
    Indices = shutdown_loop(sets:new(), 0, length(Handles)),
    io:format("Number of messages received by subscribers: ~p~n", [sets:to_list(Indices)]).


%%====================================================================
%% Internal functions
%%====================================================================


subscription_loop(Parent, Channel, Idx) ->
    receive
        %% This is the first message received
        #'basic.consume_ok'{} ->
            subscription_loop(Parent, Channel, Idx);

        %% This is received when the subscription is cancelled
        #'basic.cancel_ok'{} ->
            Parent ! Idx;

        %% A delivery
        {#'basic.deliver'{delivery_tag = Tag}, _Content} ->
            %% Do something with the message payload
            %% (some work here)
            io:format("~p received message (#~p)~n", [self(), Idx + 1]),
            %io:format("Message received: ~p", [Content]),

            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

            %% Loop
            subscription_loop(Parent, Channel, Idx + 1)
    end.


startup_loop(Hds, N) ->
    NumHds = length(Hds),
    case NumHds == N of
        true ->
            Hds;
        false ->
            receive
                Hd ->
                    io:format("Started ~p~n", [Hd]),
                    startup_loop([Hd | Hds], N)
            end
    end.


shutdown_loop(Idxs, Complete, N) ->
    case Complete == N of
        true ->
            Idxs;
        false ->
            receive
                Idx ->
                    shutdown_loop(sets:add_element(Idx, Idxs), Complete + 1, N)
            end
    end.


run_many_helper(Creds, Parent, Idx, N) when Idx < N ->
    spawn(fun() -> run(Creds, Parent) end),
    timer:sleep(1),
    run_many_helper(Creds, Parent, Idx + 1, N);
run_many_helper(_Creds, _Parent, Idx, N) when Idx == N ->
    ok.

