-module(dproto_tcp).

-define(BUCKET_SIZE, 8).
-define(METRIC_ELEM_SIZE, 8).
-define(METRIC_SIZE, 16).

-export([encode_metrics/1, decode_metrics/1, encode_get/4, decode_get/1, decode_list/1]).
-ignore_xref([encode_metrics/1, decode_metrics/1, encode_get/4, decode_get/1]).

encode_metrics(Ms) ->
    Data = << <<(byte_size(M)):?METRIC_SIZE/integer, M/binary>> ||  M <- Ms >>,
    <<(byte_size(Data)):32/integer, Data/binary>>.

decode_metrics(<<S:32/integer, Ms:S/binary>>) ->
    decode_metrics(Ms, []).

decode_metrics(<<>>, Acc) ->
    Acc;

decode_metrics(<<S:?METRIC_SIZE/integer, M:S/binary, R/binary>>, Acc) ->
    decode_metrics(R, [M | Acc]).

decode_get(<<_LB:?BUCKET_SIZE/integer, B:_LB/binary,
             _LM:?METRIC_SIZE/integer, M:_LM/binary,
             T:64/integer, C:32/integer>>) ->
    {B, M, T, C}.

encode_get(B, M, T, C) ->
    <<(byte_size(B)):?BUCKET_SIZE/integer, B/binary,
      (byte_size(M)):?METRIC_SIZE/integer, M/binary,
      T:64/integer, C:32/integer>>.


decode_list(<<_LB:?BUCKET_SIZE/integer, B:_LB/binary>>) ->
    B.

%% decode_metric(<<_L:?METRIC_ELEM_SIZE/integer, M:_L/binary, R/binary>>, Acc) ->
%%     decode_metric(R, [M, Acc]);
%% decode_metric(<<>>, Acc) ->
%%     lists:reverse(Acc).

