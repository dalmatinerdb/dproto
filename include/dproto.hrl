-define(PUT, 0).
-define(LIST, 1).
-define(GET, 2).
-define(BUCKETS, 3).
-define(STREAM, 4).

-define(SENTRY, 5).
-define(SWRITE, 6).
-define(BUCKET_INFO, 7).
-define(BUCKET_ADD, 8).
-define(BUCKET_DELETE, 9).
-define(SBATCH, 10).
-define(LIST_PREFIX, 11).
-define(TTL, 12).

-define(EVENTS, 13).
-define(GET_EVENTS, 14).
-define(REPLY_EVENTS, 15).
-define(END_EVENTS, 16).
-define(GET_EVENTS_FILTERED, 17).

-define(ERROR, 255).

%% number of bits used to encode the bucket size.
%% => buckets can be 255 byte at most!
-define(BUCKET_SS, 8).

%% number of bits used to encode the entire buckets list.
-define(BUCKETS_SS, 32).

%% The number of bits used to encode the size of the a single metric part.
%% => each part can be 255 byte at max!
-define(METRIC_ELEMENT_SS, 8).

%% The number of bits used to encode the size of a whole metric (All of it's
%% parts)
%% => the maximum number of bytes in a whole metric can be 65,536, so a single
%% metric can hold at least 255 elments, more if their size is < 256 byte.
-define(METRIC_SS, 16).

%% The number of bits used to encode a list of metrics.
%% => this means a list opperation can return at least 281,474,976,710,656
%% metrics.
%%
%% That is a lot! Good problem to have if we ever face it!
-define(METRICS_SS, 64).

%% The number of bits used to encode the length of the payload data.
%% => this means we can encode 4,294,967,296 byte or 536,870,912 points
%% at 8 byte / point in a single request.
%%
%% we should never do that!
-define(DATA_SS, 32).

%% The number of bits used for encoding the time.
-define(TIME_SIZE, 64).
%% The number of bits used for encoding the count.
-define(COUNT_SIZE, 32).

%% Number of bits used to encode the delay as part of the streaming protocol.
-define(DELAY_SIZE, 8).

%% Number of bits needed to control consistency parameters for read operations
-define(GET_OPT_SIZE, 8).

%% Disable, enable or use the default read repair option for a get request.
-define(RR_OFF, 0).
-define(RR_ON, 1).
-define(RR_DEFAULT, 2).

%% The type used to encode sizes.
-define(SIZE_TYPE, unsigned-integer).

%% The type used to encode time.
-define(TIME_TYPE, unsigned-integer).

%% The type used to encode error messages.
-define(ERROR_SIZE, 8).
