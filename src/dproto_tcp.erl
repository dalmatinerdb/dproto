-module(dproto_tcp).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         encode_metrics/1, decode_metrics/1,
         encode_buckets/1, decode_buckets/1,
         encode_bucket_info/3, decode_bucket_info/1,
         encode/1,
         decode/1,
         decode_stream/1,
         decode_batch/1
        ]).

-export_type([tcp_message/0, batch_message/0, stream_message/0]).

-type ttl() :: pos_integer() | infinity.

-type stream_message() ::
        flush |
        incomplete |
        {batch,
         Time :: non_neg_integer()} |
        {stream,
         Metric :: binary(),
         Time :: non_neg_integer(),
         Points :: binary()}.

-type batch_message() ::
        incomplete |
        batch_end |
        {batch,
         Metric :: binary(),
         Points :: binary()}.

-type tcp_message() ::
        buckets |
        {ttl, Bucket :: binary(), TTL :: ttl()} |
        {list, Bucket :: binary()} |
        {list, Bucket :: binary(), Prefix :: binary()} |
        {delete, Bucket :: binary()} |
        {get,
         Bucket :: binary(),
         Metric :: binary(),
         Time :: pos_integer(),
         Count :: pos_integer()} |
        {stream,
         Bucket :: binary(),
         Delay :: pos_integer()}.

-type encoded_metric() :: <<_:?METRICS_SS,_:_*8>>.
-type encoded_bucket() :: <<_:?BUCKETS_SS,_:_*8>>.
%%--------------------------------------------------------------------
%% @doc
%% Encode a list of metrics to its binary form for sending it over
%% the wire.
%%
%% @end
%%--------------------------------------------------------------------

-spec encode_metrics([dproto:metric()]) ->
                            encoded_metric().

encode_metrics(Metrics) when is_list(Metrics) ->
    Data = << <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary>>
              ||  Metric <- Metrics >>,
    <<(byte_size(Data)):?METRICS_SS/?SIZE_TYPE, Data/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes the binary representation of a metric list to its list
%% representation.
%%
%% Node this does not recursively decode the metrics!
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_metrics(encoded_metric()) ->
                            [dproto:metric()].

decode_metrics(<<_Size:?METRICS_SS/?SIZE_TYPE, Metrics:_Size/binary>>) ->
    [ Metric || <<_S:?METRIC_SS/?SIZE_TYPE, Metric:_S/binary>> <= Metrics].

%%--------------------------------------------------------------------
%% @doc
%% Encode a list of buckets to its binary form for sending it over
%% the wire.
%%
%% @end
%%--------------------------------------------------------------------

-spec encode_buckets([dproto:metric()]) ->
                            encoded_bucket().

encode_buckets(Buckets) when is_list(Buckets) ->
    Data = << <<(byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>
              ||  Bucket <- Buckets >>,
    <<(byte_size(Data)):?BUCKETS_SS/?SIZE_TYPE, Data/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes the binary representation of a bucket list to its list
%% representation.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_buckets(encoded_bucket()) ->
                            [dproto:bucket()].

decode_buckets(<<_Size:?BUCKETS_SS/?SIZE_TYPE, Buckets:_Size/binary>>) ->
    [ Bucket || <<_S:?BUCKET_SS/?SIZE_TYPE, Bucket:_S/binary>> <= Buckets].

%%--------------------------------------------------------------------
%% @doc
%% Encodes bucket properties such as PPF, Resolution and TTL into a binary
%% form for transmission over the wire.
%%
%% @end
%%--------------------------------------------------------------------

-spec encode_bucket_info(pos_integer(), pos_integer(), ttl()) ->
                            <<_:192>>.

encode_bucket_info(Resolution, PPF, _TTL) when
      is_integer(Resolution), Resolution > 0,
      is_integer(PPF), PPF > 0,
      _TTL =:= infinity ->
    <<Resolution:?TIME_SIZE/?TIME_TYPE,
      PPF:?TIME_SIZE/?TIME_TYPE,
      0:?TIME_SIZE/?TIME_TYPE>>;
encode_bucket_info(Resolution, PPF, TTL) when
      is_integer(Resolution), Resolution > 0,
      is_integer(PPF), PPF > 0,
      is_integer(TTL), TTL > 0 ->
    <<Resolution:?TIME_SIZE/?TIME_TYPE,
      PPF:?TIME_SIZE/?TIME_TYPE,
      TTL:?TIME_SIZE/?TIME_TYPE>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes bucket properties from the wire protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_bucket_info(binary()) ->
                            {pos_integer(), pos_integer(), ttl()}.

decode_bucket_info(<<Resolution:?TIME_SIZE/?TIME_TYPE,
                     PPF:?TIME_SIZE/?TIME_TYPE,
                     TTL:?TIME_SIZE/?TIME_TYPE>>) ->
    case TTL of
        0 ->
            {Resolution, PPF, infinity};
        _ ->
            {Resolution, PPF, TTL}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Encodes a message for the binary protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec encode(tcp_message() | stream_message() | batch_message()) ->
                    binary().
encode(buckets) ->
    <<?BUCKETS>>;

%% @doc
%% Encodes the TTL for a bucket.
%% Note that a zero value is substituted in place of `infinity'.
%%
%% @end
encode({ttl, Bucket, infinity}) ->
    <<?TTL,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      0:?TIME_SIZE/?TIME_TYPE>>;
encode({ttl, Bucket, TTL}) when is_binary(Bucket), byte_size(Bucket) > 0,
                                is_integer(TTL), TTL > 0 ->
    <<?TTL,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      TTL:?TIME_SIZE/?TIME_TYPE>>;

encode({list, Bucket}) when is_binary(Bucket), byte_size(Bucket) > 0 ->
    <<?LIST,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary>>;

encode({list, Bucket, Prefix}) when is_binary(Bucket), byte_size(Bucket) > 0,
                                    is_binary(Prefix), byte_size(Prefix) > 0 ->
    <<?LIST_PREFIX,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      (byte_size(Prefix)):?METRIC_SS/?SIZE_TYPE, Prefix/binary>>;

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

encode({stream, Bucket, Delay, Resolution}) when
      is_binary(Bucket), byte_size(Bucket) > 0,
      is_integer(Delay), Delay > 0, Delay < 256,
      Resolution > 0 ->
    <<?STREAM,
      Delay:?DELAY_SIZE/?SIZE_TYPE,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      Resolution:?TIME_SIZE/?TIME_TYPE>>;

encode({stream, Metric, Time, Points}) when
      is_binary(Metric), byte_size(Metric) > 0,
      is_binary(Points), byte_size(Points) rem ?DATA_SIZE == 0,
      is_integer(Time), Time >= 0->
    <<?SENTRY,
      Time:?TIME_SIZE/?SIZE_TYPE,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      (byte_size(Points)):?DATA_SS/?SIZE_TYPE, Points/binary>>;

encode({batch, Time}) when
      is_integer(Time), Time >= 0 ->
    <<?SBATCH,
      Time:?TIME_SIZE/?SIZE_TYPE>>;

encode({batch, Metric, Point}) when
      is_binary(Metric), byte_size(Metric) > 0,
      is_binary(Point), byte_size(Point) == ?DATA_SIZE  ->
    <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary, Point:?DATA_SIZE/binary>>;


encode({batch, Metric, Point}) when
      is_binary(Metric), byte_size(Metric) > 0,
      is_integer(Point) ->
    PointB = mmath_bin:from_list([Point]),
    <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary, PointB:?DATA_SIZE/binary>>;

encode(batch_end) ->
    <<0:?METRIC_SS/?SIZE_TYPE>>;

encode(flush) ->
    <<?SWRITE>>.


%%--------------------------------------------------------------------
%% @doc
%% Decodes a normal TCP message from the wire protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode(binary()) ->
                    tcp_message().

decode(<<?BUCKETS>>) ->
    buckets;

%% @doc
%% Decodes the TTL for a bucket.
%% Note that a zero value is interpreted to mean `infinity'.
%%
%% @end
decode(<<?TTL, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary,
         TTL:?TIME_SIZE/?TIME_TYPE>>) ->
    case TTL of
        0 ->
            {ttl, Bucket, infinity};
        _ when TTL > 0 ->
            {ttl, Bucket, TTL}
    end;

decode(<<?LIST, _Size:?BUCKET_SS/?SIZE_TYPE, Bucket:_Size/binary>>) ->
    {list, Bucket};

decode(<<?LIST_PREFIX, _BSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BSize/binary,
         _PSize:?METRIC_SS/?SIZE_TYPE, Prefix:_PSize/binary>>) ->
    {list, Bucket, Prefix};

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
    {stream, Bucket, Delay};

decode(<<?STREAM,
         Delay:?DELAY_SIZE/?SIZE_TYPE,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary,
         Resolution:?TIME_SIZE/?TIME_TYPE>>) ->
    {stream, Bucket, Delay, Resolution}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes a streaming TCP message from the wire protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_stream(binary()) ->
                           {stream_message(), binary()}.

decode_stream(<<?SWRITE, Rest/binary>>) ->
    {flush, Rest};

decode_stream(<<?SENTRY,
                Time:?TIME_SIZE/?SIZE_TYPE,
                _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
                _PointsSize:?DATA_SS/?SIZE_TYPE, Points:_PointsSize/binary,
                Rest/binary>>) ->
    {{stream, Metric, Time, Points}, Rest};

decode_stream(<<?SBATCH,
                Time:?TIME_SIZE/?SIZE_TYPE, Rest/binary>>) ->
    {{batch, Time}, Rest};

decode_stream(Rest) when is_binary(Rest) ->
    {incomplete, Rest}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes a batched TCP message from the wire protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_batch(binary()) ->
                          {batch_message(), binary()}.

decode_batch(<<0:?METRIC_SS/?SIZE_TYPE, Rest/binary>>) ->
    {batch_end, Rest};

decode_batch(<<_MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
               Point:?DATA_SIZE/binary, Rest/binary>>) ->
    {{batch, Metric, Point}, Rest};

decode_batch(Rest) ->
    {incomplete, Rest}.
