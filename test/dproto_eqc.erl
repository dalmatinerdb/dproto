-module(dproto_eqc).

-ifdef(TEST).
-ifdef(EQC).

-define(EQC_NUM_TESTS, 5000).

-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("fqc/include/fqc.hrl").
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
            Metrics == dproto:decode_metrics(dproto:encode_metrics(Metrics))).


-endif.
-endif.
