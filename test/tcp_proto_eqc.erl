-module(tcp_proto_eqc).

-ifdef(TEST).
-ifdef(EQC).

-define(EQC_NUM_TESTS, 5000).

-include_lib("mmath/include/mmath.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("fqc/include/fqc.hrl").
-compile(export_all).


non_empty_binary() ->
    ?SUCHTHAT(B, ?LET(L, list(choose($a, $z)), list_to_binary(L)), B =/= <<>>).

non_empty_binary_list() ->
    ?SUCHTHAT(L, list(non_empty_binary()), L =/= []).

bucket() ->
    non_empty_binary().

metric() ->
    non_empty_binary().

neg_or_zero() ->
    ?SUCHTHAT(I, int(), I =< 0).

good_delay() ->
    ?SUCHTHAT(D, int(), D > 0 andalso (D band 16#FF) =:= D ).

bad_delay() ->
    ?SUCHTHAT(D, int(), D =< 0 orelse (D band 16#FF) =/= D).

delay() ->
    fault(bad_delay(), good_delay()).

bad_count() ->
    oneof([
           neg_or_zero(),
           choose(16#FFFFFFFF+1, 16#FFFFFFFF+1000)
          ]).

good_count() ->
    choose(1, 16#FFFFFFFF).

count() ->
    fault(bad_count(), good_count()).


bad_time() ->
    oneof([
           neg_or_zero(),
           choose(16#FFFFFFFFFFFFFFFF+1, 16#FFFFFFFFFFFFFFFF+1000)
          ]).

good_time() ->
    choose(1, 16#FFFFFFFFFFFFFFFF).

mtime() ->
    fault(bad_time(), good_time()).


point() ->
    fault(bad_point(), good_point()).

good_point() ->
    ?LET(Point, int(), mmath_bin:from_list([Point])).

bad_point() ->
    ?SUCHTHAT(Points, non_empty_binary(), byte_size(Points) /= ?DATA_SIZE).

good_points() ->
    ?LET(Points, list(int()), mmath_bin:from_list(Points)).

bad_points() ->
    ?SUCHTHAT(Points, non_empty_binary(), byte_size(Points) rem ?DATA_SIZE =/= 0).

points() ->
    fault(bad_points(), good_points()).

non_neg_int() ->
    ?SUCHTHAT(I, int(), I >= 0).

pos_int() ->
    ?SUCHTHAT(I, non_neg_int(), I > 0).

tcp_msg() ->
    oneof([
           buckets,
           {list, bucket()},
           {list, bucket(), metric()},
           {info, bucket()},
           {add, bucket(), pos_int(), pos_int(), non_neg_int()},
           {delete, bucket()}
          ]).

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
    ?FORALL(Msg = {get, _, _, Time, Count},
            {get, bucket(), metric(), mtime(), count()},
            case valid_time(Time) andalso valid_count(Count) of
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

prop_encode_decode_metrics() ->
    ?FORALL(L, non_empty_binary_list(),
            begin
                L1 = lists:sort(L),
                B = dproto_tcp:encode_metrics(L),
                Rev = dproto_tcp:decode_metrics(B),
                L2 = lists:sort(Rev),
                L1 == L2
            end).

-endif.
-endif.
