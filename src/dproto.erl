-module(dproto).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         metric_from_list/1,
         metric_to_list/1,
         metric_to_string/2,
         encode_metrics/1,
         decode_metrics/1
        ]).

-export_type([metric_list/0, metric/0]).

-type metric_list() :: [binary()].
-type metric() :: binary().

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
%% @spec metric_to_string(metric(), binary()) ->
%%                              binary()
%%
%% @end
%%--------------------------------------------------------------------

-spec metric_to_string(metric(), binary()) ->
                              binary().

metric_to_string(Metric, Seperator) ->
    SepSize = byte_size(Seperator),
    <<Seperator:SepSize/binary, Result/binary>> =
        << <<Seperator/binary, M/binary>> ||
            <<_Size:?METRIC_ELEMENT_SS/?SIZE_TYPE, M:_Size/binary>>
                <= Metric >>,
    Result.

%%--------------------------------------------------------------------
%% @doc
%% Encodes a list of metrics to it's binary representaiton.
%%
%% @spec encode_metrics(Metrics :: [binary()]) ->
%%                             binary()
%% @end
%%--------------------------------------------------------------------

-spec encode_metrics(Metrics :: [binary()]) ->
                            binary().

encode_metrics(Metrics) when is_list(Metrics) ->
    << <<(byte_size(Metric)):?METRIC_SS/?SIZE_TYPE, Metric/binary>>
       || Metric <- Metrics >>.

%%--------------------------------------------------------------------
%% @doc
%% Decodes a binary representation of a list of metrics to a list.
%%
%% @spec decode_metrics(Metrics :: binary()) ->
%%                             [binary()]
%%
%% @end
%%--------------------------------------------------------------------

-spec decode_metrics(Metrics :: binary()) ->
                            [binary()].

decode_metrics(Metrics) when is_binary(Metrics) ->
    [Metric || <<_MetricSize:?METRIC_SS/?SIZE_TYPE, Metric:_MetricSize/binary>>
                   <= Metrics ].
