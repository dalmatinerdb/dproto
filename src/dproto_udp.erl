-module(dproto_udp).
-include("dproto.hrl").

-export([encode_header/1, encode_points/3]).

encode_header(Bucket) ->
    <<?PUT, (byte_size(Bucket)):?BUCKET_SS/integer,
      Bucket/binary>>.

encode_points(Metric, Time, Point) when is_integer(Point) ->
    encode_points(Metric, Time, <<1, Point:64/integer>>);

encode_points(Metric, Time, Points) when is_list(Points) ->
    encode_points(Metric, Time, << <<1, V:64/integer>> || V <-  Points >>);

encode_points(Metric, Time, Points) when is_binary(Points) ->
    <<Time:?TIME_SIZE/integer,
      (byte_size(Metric)):?METRIC_SS/integer, Metric/binary,
      (byte_size(Points)):?DATA_SS/integer, Points/binary>>.
