-module(dproto_tcp).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         encode_metrics/1, decode_metrics/1,
         encode_buckets/1, decode_buckets/1,
         encode_bucket_info/3, decode_bucket_info/1,
         encode/1,
         decode/1,
         decode_stream/1
        ]).

-type stream_message() ::
        {stream,
         Metric :: binary(),
         Time :: pos_integer(),
         Points :: binary()} |
        flush.

-type tcp_message() ::
        buckets |
        {list, Bucket :: binary()} |
        {get,
         Bucket :: binary(),
         Metric :: binary(),
         Time :: pos_integer(),
         Count :: pos_integer()} |
        {stream,
         Bucket :: binary(),
         Delay :: pos_integer()}.

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
    [ Metric || <<_S:?METRIC_SS/?SIZE_TYPE, Metric:_S/binary>> <= Metrics].

%%--------------------------------------------------------------------
%% @doc
%% Encode a list of buckets to it's binary form for sending it over
%% the wire.
%%
%% @spec encode_buckets([dproto:bucket()]) ->
%%                             binary().
%%
%% @end
%%--------------------------------------------------------------------

-spec encode_buckets([dproto:metric()]) ->
                            binary().

encode_buckets(Buckets) when is_list(Buckets) ->
    Data = << <<(byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>
              ||  Bucket <- Buckets >>,
    <<(byte_size(Data)):?BUCKETS_SS/?SIZE_TYPE, Data/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes the binary representation of a bucket list to it's list
%% representation.
%%
%% @spec decode_buckets(binary()) ->
%%                             [dproto:bucket()].
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_buckets(binary()) ->
                            [dproto:bucket()].

decode_buckets(<<_Size:?BUCKETS_SS/?SIZE_TYPE, Buckets:_Size/binary>>) ->
    [ Bucket || <<_S:?BUCKET_SS/?SIZE_TYPE, Bucket:_S/binary>> <= Buckets].


encode_bucket_info(Resolution, PPF, TTL) when
      is_integer(Resolution), Resolution > 0,
      is_integer(PPF), PPF > 0,
      is_integer(TTL), TTL >= 0 ->
    <<Resolution:?TIME_SIZE/?TIME_TYPE,
      PPF:?TIME_SIZE/?TIME_TYPE,
      TTL:?TIME_SIZE/?TIME_TYPE>>.

decode_bucket_info(<<Resolution:?TIME_SIZE/?TIME_TYPE,
                     PPF:?TIME_SIZE/?TIME_TYPE,
                     TTL:?TIME_SIZE/?TIME_TYPE>>) ->
    {Resolution, PPF, TTL}.
%%--------------------------------------------------------------------
%% @doc
%% Encodes a message for the binary protocol.
%%
%% @spec encode(tcp_message() | stream_message()) ->
%%                     binary()
%%
%% @end
%%--------------------------------------------------------------------

-spec encode(tcp_message() | stream_message()) ->
                    binary().
encode(buckets) ->
    <<?BUCKETS>>;

encode({list, Bucket}) when is_binary(Bucket), byte_size(Bucket) > 0 ->
    <<?LIST,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({info, Bucket}) when is_binary(Bucket), byte_size(Bucket) > 0 ->
    <<?BUCKET_INFO,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({add, Bucket, Resolution, PPF, TTL}) when
      is_binary(Bucket), byte_size(Bucket) > 0,
      is_integer(Resolution), Resolution > 0,
      is_integer(PPF), PPF > 0,
      is_integer(TTL), TTL >= 0 ->
    <<?BUCKET_ADD, (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      Resolution:?TIME_SIZE/?TIME_TYPE,
      PPF:?TIME_SIZE/?TIME_TYPE,
      TTL:?TIME_SIZE/?TIME_TYPE>>;

encode({delete, Bucket}) when is_binary(Bucket), byte_size(Bucket) > 0 ->
    <<?BUCKET_DELETE,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({get, Bucket, Metric, Time, Count}) when
      is_binary(Bucket), byte_size(Bucket) > 0,
      is_binary(Metric), byte_size(Metric) > 0,
      is_integer(Time), Time >= 0, (Time band 16#FFFFFFFFFFFFFFFF) =:= Time,
      %% We only want positive numbers <  32 bit
      is_integer(Count), Count > 0, (Count band 16#FFFFFFFF) =:= Count ->
    <<?GET,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE>>;

encode({stream, Bucket, Delay}) when
      is_binary(Bucket), byte_size(Bucket) > 0,
      is_integer(Delay), Delay > 0, Delay < 256->
    <<?STREAM,
      Delay:?DELAY_SIZE/?SIZE_TYPE,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({stream, Metric, Time, Points}) when
      is_binary(Metric), byte_size(Metric) > 0,
      is_binary(Points), byte_size(Points) rem ?DATA_SIZE == 0,
      is_integer(Time), Time >= 0->
    <<?SENTRY,
      Time:?TIME_SIZE/?SIZE_TYPE,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      (byte_size(Points)):?DATA_SS/?SIZE_TYPE, Points/binary>>;

encode(flush) ->
    <<?SWRITE>>.


%%--------------------------------------------------------------------
%% @doc
%% Decodes a normal TCP message from the wire protocol.
%%
%% @spec decode(binary()) ->
%%                     tcp_message()
%%
%% @end
%%--------------------------------------------------------------------

-spec decode(binary()) ->
                    tcp_message().

decode(<<?BUCKETS>>) ->
    buckets;

decode(<<?LIST, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary>>) ->
    {list, Bucket};

decode(<<?BUCKET_INFO, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary>>) ->
    {info, Bucket};

decode(<<?BUCKET_ADD, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary,
         Resolution:?TIME_SIZE/?TIME_TYPE,
         PPF:?TIME_SIZE/?TIME_TYPE,
         TTL:?TIME_SIZE/?TIME_TYPE>>) ->
    {add, Bucket, Resolution, PPF, TTL};

decode(<<?BUCKET_DELETE, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary>>) ->
    {delete, Bucket};

decode(<<?GET,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary,
         _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
         Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE>>) ->
    {get, Bucket, Metric, Time, Count};

decode(<<?STREAM,
         Delay:?DELAY_SIZE/?SIZE_TYPE,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary>>) ->
    {stream, Bucket, Delay}.


%%--------------------------------------------------------------------
%% @doc
%% Decodes a streaming TCP message from the wire protocol.
%%
%% @spec decode(binary()) ->
%%                     tcp_message()
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_stream(binary()) ->
                           {stream_message() | incomplete, binary()}.

decode_stream(<<?SWRITE, Rest/binary>>) ->
    {flush, Rest};

decode_stream(<<?SENTRY,
                Time:?TIME_SIZE/?SIZE_TYPE,
                _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
                _PointsSize:?DATA_SS/?SIZE_TYPE, Points:_PointsSize/binary,
                Rest/binary>>) ->
    {{stream, Metric, Time, Points}, Rest};

decode_stream(<<>>) ->
    {incomplete, <<>>};

decode_stream(<<?SENTRY, _/binary>> = Rest) ->
    {incomplete, Rest}.
