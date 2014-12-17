-module(dproto).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         encode_points/1,
         metric_from_list/1,
         metric_to_list/1,
         metric_to_string/2
        ]).

-export_type([metric_list/0, metric/0]).

-type metric_list() :: [binary()].
-type metric() :: binary().


%%--------------------------------------------------------------------
%% @doc
%% Encodes either a list of points or a single point into the
%% dalmatiner binary representation.
%%
%% @spec encode_points(integer() | [integer()] | binary()) ->
%%                            binary()
%%
%% @end
%%--------------------------------------------------------------------

-spec encode_points(integer() | [integer()] | binary()) ->
                           binary().

encode_points(Point) when is_integer(Point) ->
    <<?INT:?TYPE_SIZE, Point:?BITS/?INT_TYPE>>;
encode_points(Points) when is_list(Points) ->
    << <<?INT:?TYPE_SIZE, V:?BITS/?INT_TYPE>> || V <-  Points >>;
encode_points(Points) when is_binary(Points) ->
    Points.


%%--------------------------------------------------------------------
%% @doc
%% Encodes the list representation of a metric to the binary
%% representation.
%%
%% @spec metric_from_list(metric_list()) ->
%%                               metric()
%%
%% @end
%%--------------------------------------------------------------------

-spec metric_from_list(metric_list()) ->
                              metric().

metric_from_list(Metric) when is_list(Metric) ->
    << <<(byte_size(M)):?METRIC_ELEMENT_SS/?SIZE_TYPE, M/binary>>
       || M <- Metric>>.

%%--------------------------------------------------------------------
%% @doc
%% Encodes the list representation of a metric to the binary
%% representation.
%%
%% @spec metric_to_list(metric()) ->
%%                             metric_list()
%%
%% @end
%%--------------------------------------------------------------------

-spec metric_to_list(metric()) ->
                            metric_list().
metric_to_list(Metric) when is_binary(Metric) ->
    [M || <<_Size:?METRIC_ELEMENT_SS/?SIZE_TYPE, M:_Size/binary>>
              <= Metric].


%%--------------------------------------------------------------------
%% @doc
%% Transforms a metric in a human readable string representation,
%% joining the data with a seperator.
%%
%% @spec metric_to_list(metric()) ->
%%                             metric_list()
%%
%% @end
%%--------------------------------------------------------------------
metric_to_string(Metric, Seperator) ->
    SepSize = byte_size(Seperator),
    <<Seperator:SepSize/binary, Result/binary>> =
        << <<Seperator/binary, M/binary>> ||
            <<_Size:?METRIC_ELEMENT_SS/?SIZE_TYPE, M:_Size/binary>>
                <= Metric >>,
    Result.
