-module(tcp_proto_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

non_empty_binary() ->
    ?SUCHTHAT(B, ?LET(L, list(choose($a, $z)), list_to_binary(L)), B =/= <<>>).

non_empty_binary_list() ->
    ?SUCHTHAT(L, list(non_empty_binary()), L =/= []).

prop_encode_decode_metrics() ->
    ?FORALL(L, non_empty_binary_list(),
            begin
                L1 = lists:sort(L),
                B = dproto_tcp:encode_metrics(L),
                Rev = dproto_tcp:decode_metrics(B),
                L2 = lists:sort(Rev),
                L1 == L2
            end).

prop_encode_decode_get() ->
    ?FORALL({B, M, T, C}, {non_empty_binary(), non_empty_binary(), choose(0, 5000), choose(1, 5000)},
            {B, M, T, C} == dproto_tcp:decode_get(dproto_tcp:encode_get(B, M, T, C))).

prop_encode_decode_list() ->
    ?FORALL(B, non_empty_binary(),
            B == dproto_tcp:decode_list(dproto_tcp:encode_list(B))).

-include("eqc_helper.hrl").
-endif.
-endif.
