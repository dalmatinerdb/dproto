-module(dproto_udp).
-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([encode_header/1, encode_points/3]).

encode_header(Bucket) ->
    <<?PUT, (byte_size(Bucket)):?BUCKET_SS/?SIZE_TYPE,
      Bucket/binary>>.

encode_points(Metric, Time, Points) when is_binary(Metric) ->
    PointsB = dproto:encode_points(Points),
    <<Time:?TIME_SIZE/?SIZE_TYPE,
      (byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary,
      (byte_size(PointsB)):?DATA_SS/?SIZE_TYPE, PointsB/binary>>.
