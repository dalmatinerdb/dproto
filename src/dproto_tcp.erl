-module(dproto_tcp).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         encode_metrics/1, decode_metrics/1,
         encode_get_req/4,
         decode_get_req/1,
         decode_list/1, encode_list/1,
         encode_start_stream/2,
         encode_stream_flush/0,
         encode_stream_payload/3,
         encode/1,
         decode/1
        ]).

%%--------------------------------------------------------------------
%% @doc
%% Encode a list of metrics to it's binary form for sending it over
%% the wire.
%%
%% @spec encode_metrics([dproto:metric()]) ->
%%                             binary().
%%
%% @end
%%--------------------------------------------------------------------

-spec encode_metrics([dproto:metric()]) ->
                            binary().

encode_metrics(Metrics) when is_list(Metrics) ->
    Data = << <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary>>
              ||  Metric <- Metrics >>,
    <<(byte_size(Data)):?METRICS_SS/?SIZE_TYPE, Data/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes the binary representation of a metric list to it's list
%% representation.
%%
%% Node this does not recursively decode the metrics!
%%
%% @spec decode_metrics(binary()) ->
%%                             [dproto:metric()].
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_metrics(binary()) ->
                            [dproto:metric()].

decode_metrics(<<_Size:?METRICS_SS/?SIZE_TYPE, Metrics:_Size/binary>>) ->
    [ Metric || <<_S:?METRIC_SS/?SIZE_TYPE, Metric :_S/binary>> <= Metrics].




encode_get_req(B, M, T, C) ->
    <<(byte_size(B)):?BUCKET_SS/?SIZE_TYPE, B/binary,
      (byte_size(M)):?METRIC_SS/?SIZE_TYPE, M/binary,
      T:?TIME_SIZE/?SIZE_TYPE, C:?COUNT_SIZE/?SIZE_TYPE>>.

decode_get_req(<<_LB:?BUCKET_SS/?SIZE_TYPE, Bucket:_LB/binary,
                 _LM:?METRIC_SS/?SIZE_TYPE, Metric:_LM/binary,
                 Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE>>) ->
    {Bucket, Metric, Time, Count}.

decode_list(<<_LB:?BUCKET_SS/?SIZE_TYPE, B:_LB/binary>>) ->
    B.

encode_list(Bucket) ->
    <<(byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>.

encode_start_stream(Delay, Bucket) when
      is_integer(Delay), Delay > 0, Delay < 256,
      is_binary(Bucket) ->
    <<?STREAM, Delay:8, Bucket/binary>>.

encode_stream_flush() ->
    <<?SWRITE>>.

encode_stream_payload(Metric, Time, Points) when is_list(Metric) ->
    encode_stream_payload(dproto:metric_from_list(Metric), Time, Points);

encode_stream_payload(Metric, Time, Points) when is_binary(Metric) ->
    PointsB = dproto:encode_points(Points),
    <<?SENTRY,
      Time:?TIME_SIZE/?SIZE_TYPE,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      (byte_size(PointsB)):?DATA_SS/?SIZE_TYPE, PointsB/binary>>.


encode(buckets) ->
    <<?BUCKETS>>;

encode({list, Bucket}) when is_binary(Bucket) ->
    <<?LIST,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({get, Bucket, Metric, Time, Count}) when
      is_binary(Bucket), is_binary(Metric),
      is_integer(Time), Time >= 0, (Time band 16#FFFFFFFFFFFFFFFF) =:= Time,
      %% We only want positive numbers <  32 bit
      is_integer(Count), Count > 0, (Count band 16#FFFFFFFF) =:= Count ->
    <<?GET,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE>>;

encode({stream, Bucket, Delay}) when
      is_binary(Bucket),
      is_integer(Delay), Delay > 0, Delay < 256->
    <<?STREAM,
      Delay:?DELAY_SIZE/?SIZE_TYPE,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({stream, Metric, Time, Points}) when
      is_binary(Metric),
      is_binary(Points), byte_size(Points) rem ?DATA_SIZE == 0,
      is_integer(Time), Time >= 0->
    <<?SENTRY,
      Time:?TIME_SIZE/?SIZE_TYPE,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      (byte_size(Points)):?DATA_SS/?SIZE_TYPE, Points/binary>>;

encode(flush) ->
    <<?SWRITE>>.

decode(<<?BUCKETS>>) ->
    buckets;

decode(<<?LIST, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary>>) ->
    {list, Bucket};

decode(<<?GET,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary,
         _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
         Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE>>) ->
    {get, Bucket, Metric, Time, Count};

decode(<<?STREAM,
         Delay:?DELAY_SIZE/?SIZE_TYPE,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary>>) ->
    {stream, Bucket, Delay};
decode(<<?SENTRY,
         Time:?TIME_SIZE/?SIZE_TYPE,
         _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
         _PointsSize:?DATA_SS/?SIZE_TYPE, Points:_PointsSize/binary>>) ->
    {stream, Metric, Time, Points};


decode(<<?SWRITE>>) ->
    flush.

