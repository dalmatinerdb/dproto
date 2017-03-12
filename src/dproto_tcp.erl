-module(dproto_tcp).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         encode_metrics/1, decode_metrics/1,
         encode_buckets/1, decode_buckets/1,
         encode_bucket_info/1, decode_bucket_info/1,
         encode/1,
         decode/1,
         encode_get_reply/1,
         encode_get_stream/1,
         decode_get_reply/1,
         decode_get_stream/2,
         decode_stream/1,
         decode_batch/1
        ]).

-ignore_xref([
         encode_metrics/1, decode_metrics/1,
         encode_buckets/1, decode_buckets/1,
         encode_bucket_info/1, decode_bucket_info/1,
         encode/1, decode/1,
         encode_get_reply/1,
         encode_get_stream/1,
         decode_get_reply/1,
         decode_get_stream/2,
         decode_stream/1,
         decode_batch/1
        ]).

-export_type([ttl/0, read_opts/0, read_repair_opt/0, read_r_opt/0,
              bucket_info/0, tcp_message/0,
              batch_message/0, stream_message/0]).

-ifdef(TEST).
-export([encode_aggr/1, decode_aggr/1]).
-endif.

-type ttl() :: pos_integer() | infinity.

-type read_repair_opt() :: {rr, default} |
                           {rr, off} |
                           {rr, on}.

-type read_r_opt() :: {r, n} |
                      {r, default} |
                      {r, 1..254}.

-type aggr() :: {binary(), pos_integer()}.

-type read_aggr_opt() :: {aggr, aggr()}.

-type read_opts() :: [read_repair_opt() | read_r_opt() | read_aggr_opt()].

-type bucket_info() :: #{
                   resolution => pos_integer(),
                   ppf        => pos_integer(),
                   grace      => non_neg_integer(),
                   ttl        => ttl()
                  }.

-type get_stream_element() ::
        {more, binary()} |
        {done, binary()}.

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

%% Messages shorthands that can be encoded but will never be decoded.
-type tcp_encode_message() ::
        {get,
         Bucket :: binary(),
         Metric :: binary(),
         Time :: pos_integer(),
         Count :: pos_integer()}.
-type tcp_message() ::
        buckets |
        {ttl, Bucket :: binary(), TTL :: ttl()} |
        {list, Bucket :: binary()} |
        {list, Bucket :: binary(), Prefix :: binary()} |
        {info, Bucket :: binary()} |
        {delete, Bucket :: binary()} |
        {events, [{pos_integer(), term()}]} |
        events_end |
        {events, Bucket :: binary(), [{pos_integer(), term()}]} |
        {get_events,
         Bucket :: binary(),
         Start  :: pos_integer(),
         End    :: pos_integer()} |
        {get_events,
         Bucket :: binary(),
         Start  :: pos_integer(),
         End    :: pos_integer(),
         Filter :: jsxd:filter_filters()} |
        {get,
         Bucket :: binary(),
         Metric :: binary(),
         Time :: pos_integer(),
         Count :: pos_integer(),
         Opts :: read_opts()} |
        {stream,
         Bucket :: binary(),
         Delay :: pos_integer()} |
        {stream,
         Bucket :: binary(),
         Delay :: pos_integer(),
         Resolution :: pos_integer()} |
        {error,
         Message :: binary()}.

-type encoded_metric() :: <<_:?METRICS_SS, _:_*8>>.
-type encoded_bucket() :: <<_:?BUCKETS_SS, _:_*8>>.
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

-spec encode_bucket_info(bucket_info()) ->
                                <<_:192>> | <<_:256>>.

encode_bucket_info(#{
                      resolution := Resolution,
                      ppf        := PPF,
                      grace      := Grace,
                      ttl        := infinity
                    }) when
      is_integer(Resolution), Resolution > 0,
      is_integer(PPF), PPF > 0,
      is_integer(Grace), Grace >= 0 ->
    <<Resolution:?TIME_SIZE/?TIME_TYPE,
      PPF:?TIME_SIZE/?TIME_TYPE,
      Grace:?TIME_SIZE/?TIME_TYPE>>;
encode_bucket_info(#{
                      resolution := Resolution,
                      ppf        := PPF,
                      grace      := Grace,
                      ttl        := TTL
                    }) when
      is_integer(Resolution), Resolution > 0,
      is_integer(PPF), PPF > 0,
      is_integer(Grace), Grace >= 0,
      is_integer(TTL), TTL > 0 ->
    <<Resolution:?TIME_SIZE/?TIME_TYPE,
      PPF:?TIME_SIZE/?TIME_TYPE,
      Grace:?TIME_SIZE/?TIME_TYPE,
      TTL:?TIME_SIZE/?TIME_TYPE>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes bucket properties from the wire protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_bucket_info(<<_:192, _:_*64>>) ->
                                bucket_info().

decode_bucket_info(<<Resolution:?TIME_SIZE/?TIME_TYPE,
                     PPF:?TIME_SIZE/?TIME_TYPE,
                     Grace:?TIME_SIZE/?TIME_TYPE>>) ->
    #{
       resolution => Resolution,
       ppf        => PPF,
       grace      => Grace,
       ttl        => infinity
     };
decode_bucket_info(<<Resolution:?TIME_SIZE/?TIME_TYPE,
                     PPF:?TIME_SIZE/?TIME_TYPE,
                     Grace:?TIME_SIZE/?TIME_TYPE,
                     TTL:?TIME_SIZE/?TIME_TYPE>>) ->
    #{
       resolution => Resolution,
       ppf        => PPF,
       grace      => Grace,
       ttl        => TTL
     }.

%%--------------------------------------------------------------------
%% @doc
%% Encodes a message for the binary protocol.
%%
%% @end
%%--------------------------------------------------------------------

-spec encode(tcp_encode_message() | tcp_message() | stream_message()
             | batch_message()) ->
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

encode({get, Bucket, Metric, Time, Count}) ->
    encode({get, Bucket, Metric, Time, Count, []});

encode({get, Bucket, Metric, Time, Count, Opts}) when
      is_binary(Bucket), byte_size(Bucket) > 0,
      is_binary(Metric), byte_size(Metric) > 0,
      is_integer(Time), Time >= 0, (Time band 16#FFFFFFFFFFFFFFFF) =:= Time,
      %% We only want positive numbers <  32 bit
      is_integer(Count), Count > 0, (Count band 16#FFFFFFFF) =:= Count,
      is_list(Opts) ->
    RROpt = proplists:get_value(rr, Opts, default),
    ROpt = proplists:get_value(r, Opts, default),
    Res = <<?GET,
            (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
            (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
            Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE,
            (encode_rr(RROpt)):?GET_OPT_SIZE/?SIZE_TYPE,
            (encode_r(ROpt)):?GET_OPT_SIZE/?SIZE_TYPE>>,
    case proplists:get_value(aggr, Opts) of
        undefined ->
            Res;
        Aggr ->
            AggrB = encode_aggr(Aggr),
            <<Res/binary, AggrB/binary>>
    end;

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
    <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      Point:?DATA_SIZE/binary>>;

encode({batch, Metric, Point}) when
      is_binary(Metric), byte_size(Metric) > 0,
      is_integer(Point) ->
    PointB = mmath_bin:from_list([Point]),
    <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      PointB:?DATA_SIZE/binary>>;

encode(batch_end) ->
    <<0:?METRIC_SS/?SIZE_TYPE>>;

encode(flush) ->
    <<?SWRITE>>;

encode({events, Bucket, Events}) ->
    EventsB = encode_events(Events),
    <<?EVENTS,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      EventsB/binary>>;

encode({get_events, Bucket, Start, End}) ->
    <<?GET_EVENTS, (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      Start:?TIME_SIZE/?TIME_TYPE, End:?TIME_SIZE/?TIME_TYPE>>;
encode({get_events, Bucket, Start, End, Filter}) ->
    <<?GET_EVENTS_FILTERED,
      (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE, Bucket/binary,
      Start:?TIME_SIZE/?TIME_TYPE, End:?TIME_SIZE/?TIME_TYPE,
      (jsxd_filter:serialize(Filter))/binary>>;
encode({events, Events}) ->
    EventsB = encode_events(Events),
    <<?REPLY_EVENTS, EventsB/binary>>;
encode(events_end) ->
    <<?END_EVENTS>>;

encode({error, Message}) ->
    <<?ERROR, (byte_size(Message)):?ERROR_SIZE/?SIZE_TYPE, Message/binary>>.

-spec encode_events([{pos_integer(), term()}]) -> binary().
encode_events(Es) ->
    {ok, B} = snappyer:compress(<< << (encode_event(E))/binary >> || E <- Es >>),
    %% Damn you dailyzer!
    true = is_binary(B),
    B.

-spec encode_event({pos_integer(), term()}) -> <<_:64, _:_*8>>.
encode_event({T, E}) when T > 0, is_integer(T) ->
    B = term_to_binary(E),
    <<T:?TIME_SIZE/?TIME_TYPE, (byte_size(B)):?DATA_SS/?SIZE_TYPE, B/binary>>.

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
    Opts = [{r, default}, {rr, default}],
    {get, Bucket, Metric, Time, Count, Opts};

decode(<<?GET,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary,
         _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
         Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE,
         RR:?GET_OPT_SIZE/?SIZE_TYPE, R:?GET_OPT_SIZE/?SIZE_TYPE>>) ->
    Opts = [{r, decode_r(R)}, {rr, decode_rr(RR)}],
    {get, Bucket, Metric, Time, Count, Opts};

decode(<<?GET,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary,
         _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
         Time:?TIME_SIZE/?SIZE_TYPE, Count:?COUNT_SIZE/?SIZE_TYPE,
         RR:?GET_OPT_SIZE/?SIZE_TYPE, R:?GET_OPT_SIZE/?SIZE_TYPE,
         AggrB/binary>>) ->
    Opts = [{r, decode_r(R)}, {rr, decode_rr(RR)}, {aggr, decode_aggr(AggrB)}],
    {get, Bucket, Metric, Time, Count, Opts};

decode(<<?STREAM,
         Delay:?DELAY_SIZE/?SIZE_TYPE,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary>>) ->
    {stream, Bucket, Delay};

decode(<<?STREAM,
         Delay:?DELAY_SIZE/?SIZE_TYPE,
         _BucketSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BucketSize/binary,
         Resolution:?TIME_SIZE/?TIME_TYPE>>) ->
    {stream, Bucket, Delay, Resolution};

decode(<<?EVENTS,
         _BSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BSize/binary,
         Events/binary>>) ->
    {events, Bucket, decode_events(Events)};

decode(<<?GET_EVENTS, _BSize:?BUCKET_SS/?SIZE_TYPE,
         Bucket:_BSize/binary, Start:?TIME_SIZE/?TIME_TYPE,
         End:?TIME_SIZE/?TIME_TYPE>>) ->
    {get_events, Bucket, Start, End};
decode(<<?GET_EVENTS_FILTERED,
         _BSize:?BUCKET_SS/?SIZE_TYPE, Bucket:_BSize/binary,
         Start:?TIME_SIZE/?TIME_TYPE, End:?TIME_SIZE/?TIME_TYPE,
         Filter/binary>>) ->
    {get_events, Bucket, Start, End, jsxd_filter:deserialize(Filter)};

decode(<<?REPLY_EVENTS, Events/binary>>) ->
    {events, decode_events(Events)};

decode(<<?END_EVENTS>>) ->
    events_end;

decode(<<?ERROR,
         _Size:?ERROR_SIZE/?SIZE_TYPE, Message:_Size/binary>>) ->
    {error, Message}.

decode_events(<<>>) ->
    [];
decode_events(Compressed) ->
    {ok, Events} = snappyer:decompress(Compressed),
    [ {T, binary_to_term(E)} ||
        <<T:?TIME_SIZE/?TIME_TYPE, _S:?DATA_SS/?SIZE_TYPE, E:_S/binary>>
            <= Events].

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
                Time:?TIME_SIZE/?TIME_TYPE,
                _MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary,
                _PointsSize:?DATA_SS/?SIZE_TYPE, Points:_PointsSize/binary,
                Rest/binary>>) ->
    {{stream, Metric, Time, Points}, Rest};

decode_stream(<<?SBATCH,
                Time:?TIME_SIZE/?TIME_TYPE, Rest/binary>>) ->
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


encode_get_reply({aggr, undefined}) ->
    <<?GET_AGGR>>;
encode_get_reply({aggr, Aggr}) ->
    AggrB = encode_aggr(Aggr),
    <<?GET_AGGR, AggrB/binary>>.

encode_get_stream({data, Data}) ->
    {ok, Compressed} = snappyer:compress(Data),
    <<?GET_DATA, Compressed/binary>>;
encode_get_stream({data, Data, 0}) ->
    {ok, Compressed} = snappyer:compress(Data),
    <<?GET_DATA, Compressed/binary>>;
encode_get_stream({data, Data, Padding}) ->
    {ok, Compressed} = snappyer:compress(Data),
    <<?GET_PADDED, Padding:?COUNT_SIZE/?SIZE_TYPE, Compressed/binary>>;
encode_get_stream(done) ->
    <<?GET_DONE>>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes streamed/compressed the initial get reply.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_get_reply(binary()) ->
                              {aggr, aggr() | undefined, get_stream_element()}.

decode_get_reply(<<?GET_AGGR>>) ->
    {aggr, undefined, {more, <<>>}};
decode_get_reply(<<?GET_AGGR, Aggr/binary>>) ->
    {aggr, decode_aggr(Aggr), {more, <<>>}};
decode_get_reply(NoAggr) ->
    Res = decode_get_reply(NoAggr),
    {aggr, undefined, Res}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes streamed/compressed consecutive get replies.
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_get_stream(binary(), binary()) ->
                              get_stream_element().
decode_get_stream(<<?GET_DONE>>, Acc) ->
    {done, Acc};
decode_get_stream(<<?GET_DATA, Compressed/binary>>, Acc) ->
    {ok, Data} = snappyer:decompress(Compressed),
    {more, <<Acc/binary, Data/binary>>};

decode_get_stream(<<?GET_PADDED, Padding:?COUNT_SIZE/?SIZE_TYPE,
                    Compressed/binary>>, Acc) ->
    {ok, Data} = snappyer:decompress(Compressed),
    {more, <<Acc/binary, Data/binary,
             (mmath_bin:empty(Padding))/binary>>};

%% Backwards compatibility
decode_get_stream(<<?GET_PADDED_OLD, Padding:64/?SIZE_TYPE,
                    Compressed/binary>>, Acc) ->
    {ok, Data} = snappyer:decompress(Compressed),
    {more, <<Acc/binary, Data/binary,
             (mmath_bin:empty(Padding))/binary>>}.

%%--------------------------------------------------------------------
%% @doc
%% Encodes/decodes read repair option for a read request
%%
%% @end
%%--------------------------------------------------------------------
encode_rr(off) ->
    ?OPT_RR_OFF;
encode_rr(on) ->
    ?OPT_RR_ON;
encode_rr(default) ->
    ?OPT_RR_DEFAULT.

decode_rr(?OPT_RR_OFF) ->
    off;
decode_rr(?OPT_RR_ON) ->
    on;
decode_rr(?OPT_RR_DEFAULT) ->
    default.

%%--------------------------------------------------------------------
%% @doc
%% Encodes/decodes read quorum(R) option for a read request
%%
%% @end
%%--------------------------------------------------------------------
encode_r(n) ->
    ?OPT_R_N;
encode_r(default) ->
    ?OPT_R_DEFAULT;
encode_r(R)
  when is_integer(R), R >= 0, (R band 16#FF) =:= R ->
    R.

decode_r(?OPT_R_N) ->
    n;
decode_r(?OPT_R_DEFAULT) ->
    default;
decode_r(R) when is_integer(R), R > 0 ->
    R.

-type aggr_bin() :: <<_:40, _:_*8>>.

-spec encode_aggr(aggr()) -> aggr_bin().
encode_aggr({Name, Count})
  when is_binary(Name), byte_size(Name) =< 255,
       Count > 0->
    NameS = byte_size(Name),
    <<NameS:?METRIC_ELEMENT_SS/?SIZE_TYPE, Name:NameS/binary,
      Count:?COUNT_SIZE/?SIZE_TYPE>>.

-spec decode_aggr(aggr_bin()) -> aggr().
decode_aggr(<<NameS:?METRIC_ELEMENT_SS/?SIZE_TYPE, Name:NameS/binary,
              Count:?COUNT_SIZE/?SIZE_TYPE>>) when Count > 0->
    {Name, Count}.
