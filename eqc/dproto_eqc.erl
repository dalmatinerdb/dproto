-module(dproto_eqc).

-include_lib("eqc/include/eqc.hrl").
-compile(export_all).



non_empty_binary() ->
    ?SUCHTHAT(B, ?LET(L, list(choose($a, $z)), list_to_binary(L)), B =/= <<>>).

non_empty_binary_list() ->
    ?SUCHTHAT(L, list(non_empty_binary()), L =/= []).

prop_encode_decode_metric() ->
    ?FORALL(Metrics, non_empty_binary_list(),
            Metrics == dproto:metric_to_list(dproto:metric_from_list(Metrics))).

prop_encode_decode_metrics() ->
    ?FORALL(Metrics, non_empty_binary_list(),
            Metrics == dproto_tcp:decode_metrics(
                         dproto_tcp:encode_metrics(Metrics))).

prop_encode_decode_buckets() ->
    ?FORALL(Buckets, non_empty_binary_list(),
            Buckets == dproto_tcp:decode_buckets(
                         dproto_tcp:encode_buckets(Buckets))).
