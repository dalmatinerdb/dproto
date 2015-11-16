-module(dproto).

-include_lib("mmath/include/mmath.hrl").
-include("dproto.hrl").

-export([
         metric_from_list/1,
         metric_to_list/1,
         metric_to_string/2
        ]).

-export_type([metric_list/0, metric/0, bucket/0]).

-type metric_list() :: [binary()].
-type metric() :: binary().
-type bucket() :: binary().

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
%% Decodes the binary representation of a metric to the list
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
%% Transforms a metric into a human readable string representation,
%% joining the data with a separator.
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
