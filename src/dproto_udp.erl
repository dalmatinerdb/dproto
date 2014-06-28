-module(dproto_udp).
-include("dproto.hrl").

-export([encode_points/4]).

encode_points(Bucket, Metric, Time, Point) when is_integer(Point) ->
    encode_points(Bucket, Metric, Time, <<1, Point:64/integer>>);

encode_points(Bucket, Metric, Time, Points) when is_list(Points) ->
    encode_points(Bucket, Metric, Time, << <<1, V:64/integer>> || V <-  Points >>);

encode_points(Bucket, Metric, Time, Points) when is_binary(Points) ->
    <<?PUT, Time:64/integer,
      (byte_size(Bucket)):?BUCKET_SIZE/integer, Bucket/binary,
      (byte_size(Metric)):?METRIC_SIZE/integer, Metric/binary,
      (byte_size(Metric)):?DATA_SIZE/integer, Points/binary>>.
