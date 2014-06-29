-module(dproto_tcp).

-include("dproto.hrl").

-export([
         encode_metrics/1, decode_metrics/1,
         encode_get/4, decode_get/1,
         decode_list/1, encode_list/1
        ]).

encode_metrics(Ms) ->
    Data = << <<(byte_size(M)):?METRIC_SS/integer, M/binary>> ||  M <- Ms >>,
    <<(byte_size(Data)):?METRICS_SS/integer, Data/binary>>.

decode_metrics(<<S:?METRICS_SS/integer, Ms:S/binary>>) ->
    decode_metrics(Ms, []).

decode_metrics(<<>>, Acc) ->
    Acc;

decode_metrics(<<S:?METRIC_SS/integer, M:S/binary, R/binary>>, Acc) ->
    decode_metrics(R, [M | Acc]).

decode_get(<<_LB:?BUCKET_SS/integer, B:_LB/binary,
             _LM:?METRIC_SS/integer, M:_LM/binary,
             T:?TIME_SIZE/integer, C:?COUNT_SIZE/integer>>) ->
    {B, M, T, C}.

encode_get(B, M, T, C) ->
    <<(byte_size(B)):?BUCKET_SS/integer, B/binary,
      (byte_size(M)):?METRIC_SS/integer, M/binary,
      T:?TIME_SIZE/integer, C:?COUNT_SIZE/integer>>.


decode_list(<<_LB:?BUCKET_SS/integer, B:_LB/binary>>) ->
    B.

encode_list(Bucket) ->
    <<(byte_size(Bucket)):?BUCKET_SS/integer, Bucket/binary>>.


%% decode_metric(<<_L:?METRIC_ELEM_SIZE/integer, M:_L/binary, R/binary>>, Acc) ->
%%     decode_metric(R, [M, Acc]);
%% decode_metric(<<>>, Acc) ->
%%     lists:reverse(Acc).
