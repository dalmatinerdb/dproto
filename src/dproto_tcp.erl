-module(dproto_tcp).

-include("dproto.hrl").

-export([
         encode_metrics/1, decode_metrics/1,
         encode_get/4, decode_get/1,
         decode_list/1, encode_list/1,
         encode_start_stream/2,
         encode_stream_flush/0,
         encode_stream_payload/3
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

encode_start_stream(Delay, Bucket) when Delay > 0, Delay < 256,
                                        is_binary(Bucket) ->
    <<?STREAM, Delay:8, Bucket/binary>>.

encode_stream_flush() ->
    <<?SWRITE>>.

encode_stream_payload(Metric, Time, Points) when is_integer(Points) ->
    encode_stream_payload(Metric, Time, encode_metrics([Points]));

encode_stream_payload(Metric, Time, Points) when is_list(Points) ->
    encode_stream_payload(Metric, Time, encode_metrics(Points));

encode_stream_payload(Metric, Time, Points) when is_binary(Points),
                                                 is_binary(Metric) ->
    <<?SENTRY,
      Time:?TIME_SIZE/integer,
      (byte_size(Metric)):?METRIC_SS/integer, Metric/binary,
      (byte_size(Points)):?DATA_SS/integer, Points/binary>>.
