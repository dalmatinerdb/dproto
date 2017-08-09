-module(tcp_proto_eqc).

-include_lib("mmath/include/mmath.hrl").
-include_lib("eqc/include/eqc.hrl").
-export([prop_encode_decode_general/0,
         prop_encode_decode_stream/0,
         prop_encode_decode_stream_resolution/0,
         prop_encode_decode_stream_entry/0,
         prop_encode_decode_batch/0,
         prop_encode_decode_batch_entry/0,
         prop_encode_decode_get/0,
         prop_encode_decode_get_opts/0,
         prop_encode_decode_metrics/0,
         prop_encode_decode_bucket_info/0,
         prop_encode_decode_ttl/0,
         prop_encode_decode_aggr/0,
         prop_encode_decode_get_reply/0,
         prop_encode_decode_get_stream/0]).

non_empty_binary() ->
    ?SUCHTHAT(B, ?LET(L, list(choose($a, $z)), list_to_binary(L)), B =/= <<>>).

non_empty_binary_list() ->
    ?SUCHTHAT(L, list(non_empty_binary()), L =/= []).

bucket() ->
    non_empty_binary().

metric() ->
    non_empty_binary().


good_delay() ->
    ?SUCHTHAT(D, int(), D > 0 andalso (D band 16#FF) =:= D ).

good_count() ->
    choose(1, 16#FFFFFFFF).

good_time() ->
    choose(1, 16#FFFFFFFFFFFFFFFF).

good_point() ->
    ?LET(Point, int(), mmath_bin:from_list([Point])).

good_points() ->
    ?LET(Points, list(int()), mmath_bin:from_list(Points)).

%% We only can use fault on non eqc-mini
-ifndef(MINI).
neg_or_zero() ->
    ?SUCHTHAT(I, int(), I =< 0).

bad_count() ->
    oneof([
           neg_or_zero(),
           choose(16#FFFFFFFF+1, 16#FFFFFFFF+1000)
          ]).

bad_time() ->
    oneof([
           neg_or_zero(),
           choose(16#FFFFFFFFFFFFFFFF+1, 16#FFFFFFFFFFFFFFFF+1000)
          ]).

bad_point() ->
    ?SUCHTHAT(Points, non_empty_binary(), byte_size(Points) /= ?DATA_SIZE).

count() ->
    fault(bad_count(), good_count()).

bad_delay() ->
    ?SUCHTHAT(D, int(), D =< 0 orelse (D band 16#FF) =/= D).

delay() ->
    fault(bad_delay(), good_delay()).

mtime() ->
    fault(bad_time(), good_time()).

point() ->
    fault(bad_point(), good_point()).

bad_points() ->
    ?SUCHTHAT(Points, non_empty_binary(), byte_size(Points) rem ?DATA_SIZE =/= 0).

points() ->
    fault(bad_points(), good_points()).
-else.
delay() ->
    good_delay().

count() ->
    good_count().

mtime() ->
    good_time().

point() ->
    good_point().

points() ->
    good_points().
-endif.


pos_int() ->
    ?SUCHTHAT(N, int(), N > 0).


non_neg_int() ->
    ?SUCHTHAT(I, int(), I >= 0).

resolution() ->
    mtime().

ttl() ->
    oneof([infinity, mtime()]).

read_repair_opt() ->
    {rr, oneof([default, on, off])}.

r_opt() ->
    {r, oneof([default, n, choose(1, 254)])}.

r_aggr() ->
    {aggr, {binary(), pos_int()}}.

read_opt() ->
    oneof([read_repair_opt(), r_opt(), r_aggr()]).

read_opts() ->
    list(read_opt()).

bucket_info() ->
    #{
       resolution => resolution(),
       ppf        => pos_int(),
       grace      => non_neg_int(),
       ttl        => ttl()
     }.

filter() ->
    oneof(
      [{'not', {'==', list(binary()), binary()}},
       {'or', {'==', list(binary()), binary()},
        {'==', list(binary()), binary()}},
       {'==', list(binary()), binary()}]).

filters() ->
    list(filter()).
get_events() ->
    ?LET({S, E}, {pos_int(), pos_int()},
         oneof(
           [
            {get_events, bucket(), min(S, E), max(S, E)},
            {get_events, bucket(), min(S, E), max(S, E), filters()}
           ])).

event() ->
    {pos_int(), binary()}.

events() ->
    ?LET(L, list(event()), lists:sort(L)).

otid() ->
    ?SUCHTHAT(M,
              oneof([{oneof([undefined, pos_int()]),
                      oneof([undefined, pos_int()])},
                     undefined]),
                    M =/= {undefined, undefined}
                   ).


tcp_msg_() ->
    oneof([
           buckets,
           {list, bucket()},
           {list, bucket(), metric()},
           {info, bucket()},
           {ttl, bucket(), ttl()},
           {add, bucket(), pos_int(), pos_int(), non_neg_int()},
           {delete, bucket()},
           get_events(),
           events_end,
           {events, events()},
           {events, bucket(), events()},
           {error, binary()}
          ]).

tcp_msg() ->
    frequency(
      [{100, tcp_msg_()},
       {  5, {ot, otid(), tcp_msg_()}}]).


valid_delay(_Delay) when is_integer(_Delay), _Delay > 0, _Delay < 256 ->
    true;
valid_delay(_) ->
    false.

valid_time(_Time) when
      is_integer(_Time), _Time >= 0,
      (_Time band 16#FFFFFFFFFFFFFFFF) =:= _Time ->
    true;

valid_time(_) ->
    false.

valid_count(_Count) when
      is_integer(_Count), _Count > 0,
      (_Count band 16#FFFFFFFF) =:= _Count ->
    true;

valid_count(_) ->
    false.

valid_points(_Points) when
      is_binary(_Points), byte_size(_Points) rem ?DATA_SIZE == 0 ->
    true;

valid_points(_) ->
    false.


valid_point(_Point) when
      is_binary(_Point), byte_size(_Point) =:= ?DATA_SIZE ->
    true;

valid_point(_) ->
    false.

prop_encode_decode_general() ->
    ?FORALL(Msg, tcp_msg(),
            begin
                Encoded = dproto_tcp:encode(Msg),
                Decoded = dproto_tcp:decode(Encoded),
                ?WHENFAIL(
                   io:format(user,
                             "~p -> ~p -> ~p~n",
                             [Msg, Encoded, Decoded]),
                   Msg =:= Decoded)
            end).

prop_encode_decode_stream() ->
    ?FORALL(Msg = {stream, _, Delay}, {stream, bucket(), delay()},
            case valid_delay(Delay) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    Decoded = dproto_tcp:decode(Encoded),
                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       Msg =:= Decoded);
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_stream_resolution() ->
    ?FORALL(Msg = {stream, _, Delay},
            {stream, bucket(), delay()},

            case valid_delay(Delay) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    Decoded = dproto_tcp:decode(Encoded),
                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       Msg =:= Decoded);
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_stream_entry() ->
    ?FORALL(Msg = {stream, _, Time, Points},
            {stream, metric(), mtime(), points()},
            case valid_time(Time) andalso valid_points(Points) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    {Decoded, <<>>} = dproto_tcp:decode_stream(Encoded),
                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       Msg =:= Decoded);
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_batch() ->
    ?FORALL(Msg = {batch, Time},
            {batch, mtime()},
            case valid_time(Time) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    {Decoded, <<>>} = dproto_tcp:decode_stream(Encoded),
                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       Msg =:= Decoded);
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_batch_entry() ->
    ?FORALL(Msg = {batch, _, Point},
            {batch, metric(), point()},
            case valid_point(Point) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    {Decoded, <<>>} = dproto_tcp:decode_batch(Encoded),
                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       Msg =:= Decoded);
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_get() ->
    ?FORALL(Msg = {get, Bucket, Metric, Time, Count},
            {get, bucket(), metric(), mtime(), count()},
            case valid_time(Time) andalso valid_count(Count) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    Decoded = dproto_tcp:decode(Encoded),
                    InOpts = [],
                    {get, Bucket, Metric, Time, Count, OutOpts} = Decoded,

                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       compare_opts(InOpts, OutOpts));
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_get_opts() ->
    ?FORALL(Msg = {get, Bucket, Metric, Time, Count, InOpts},
            {get, bucket(), metric(), mtime(), count(), read_opts()},
            case valid_time(Time) andalso valid_count(Count) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    Decoded = dproto_tcp:decode(Encoded),
                    {get, Bucket, Metric, Time, Count, OutOpts} = Decoded,

                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       compare_opts(InOpts, OutOpts));
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).

prop_encode_decode_metrics() ->
    ?FORALL(L, non_empty_binary_list(),
            begin
                L1 = lists:sort(L),
                B = dproto_tcp:encode_metrics(L),
                Rev = dproto_tcp:decode_metrics(B),
                L2 = lists:sort(Rev),
                L1 == L2
            end).


prop_encode_decode_bucket_info() ->
    ?FORALL(Msg, bucket_info(),
            begin
                Encoded = dproto_tcp:encode_bucket_info(Msg),
                Decoded = dproto_tcp:decode_bucket_info(Encoded),
                ?WHENFAIL(
                   io:format(user,
                             "~p -> ~p -> ~p~n",
                             [Msg, Encoded, Decoded]),
                   Msg =:= Decoded)
            end).

prop_encode_decode_ttl() ->
    ?FORALL(Msg = {ttl, _, TTL}, {ttl, bucket(), ttl()},
            case TTL =:= infinity orelse valid_time(TTL) of
                true ->
                    Encoded = dproto_tcp:encode(Msg),
                    Decoded = dproto_tcp:decode(Encoded),
                    ?WHENFAIL(
                       io:format(user,
                                 "~p -> ~p -> ~p~n",
                                 [Msg, Encoded, Decoded]),
                       Msg =:= Decoded);
                _ ->
                    {'EXIT', _} = (catch dproto_tcp:encode(Msg))
            end).


prop_encode_decode_aggr() ->
    ?FORALL({aggr, Aggr}, r_aggr(),
            Aggr == dproto_tcp:decode_aggr(
                      dproto_tcp:encode_aggr(Aggr))).

prop_encode_decode_get_reply() ->
    ?FORALL(Aggr = {aggr, A}, oneof([r_aggr(), {aggr, undefined}]),
            begin
                Encoded = dproto_tcp:encode_get_reply(Aggr),
                {aggr, A, {more, <<>>}} == dproto_tcp:decode_get_reply(Encoded)
            end).



stream_msg() ->
    oneof([{data, good_points()},
           {data, good_points(), pos_int()}]).

prop_encode_decode_get_stream() ->
    ?FORALL(Stream, stream_msg(),
            begin
                Encoded = dproto_tcp:encode_get_stream(Stream),
                {more, Out} =  dproto_tcp:decode_get_stream(Encoded, <<>>),
                case Stream of
                    {data, Data} ->
                        Data =:= Out;
                    {data, Data, Padding} ->
                        Empty = mmath_bin:empty(Padding),
                        Data1 = <<Data/binary, Empty/binary>>,
                        Out =:= Data1
                end
            end).

compare_opts(In, Out) ->
    KnownKeys = ordsets:from_list([rr, r, aggr]),
    UKeys = ordsets:from_list(proplists:get_keys(In ++ Out)),
    OutKeys = ordsets:from_list(proplists:get_keys(Out)),
    proplists:get_value(r, In, default) =:= proplists:get_value(r, Out, default) andalso
    proplists:get_value(rr, In, default) =:= proplists:get_value(rr, Out, default) andalso
    proplists:get_value(aggr, In, none) =:= proplists:get_value(aggr, Out, none) andalso
    ordsets:is_subset(OutKeys, KnownKeys) andalso
    UKeys =:= OutKeys.
