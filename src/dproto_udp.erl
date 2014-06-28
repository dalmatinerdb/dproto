-module(dproto_udp).
-include("dproto.hrl").

-export([encode_points/4]).

encode_points(Bucket, Metric, Time, Point) when is_integer(Point) ->
    encode_points(Bucket, Metric, Time, <<1, Point:64/integer>>);

encode_points(Bucket, Metric, Time, Points) when is_list(Points) ->
    encode_points(Bucket, Metric, Time, << <<1, V:64/integer>> || V <-  Points >>);

encode_points(Bucket, Metric, Time, Points) when is_binary(Points) ->
    <<?PUT, Time:?TIME_SIZE/integer,
      (byte_size(Bucket)):?BUCKET_SS/integer, Bucket/binary,
      (byte_size(Metric)):?METRIC_SS/integer, Metric/binary,
      (byte_size(Metric)):?DATA_SS/integer, Points/binary>>.
