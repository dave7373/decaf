-module(kafka_protocol).

%% Some ideas are taken from
%% https://github.com/wooga/kafka-erlang.git


-export([producer_request/2]).


-define(RQ_TYPE_PRODUCE, 0).
-define(DEFAULT_PARTION, 0).
-define(DEFAULT_MAGIC, 1).
-define(DEFAULT_COMPRESSION, 0).

%%%-------------------------------------------------------------------
%%%                         API
%%%-------------------------------------------------------------------

-spec producer_request(Topic::binary(), Messages::list(binary())) -> binary().
producer_request(Topic,  Messages) ->
   producer_request(Topic,
                    ?DEFAULT_PARTION, ?DEFAULT_MAGIC, ?DEFAULT_COMPRESSION,
                    Messages).


%%%-------------------------------------------------------------------
%%%                  INTERNAL FUNCTIONS
%%%-------------------------------------------------------------------

-spec producer_request(Topic::binary(),
      	              Partition::integer(),
		      Magic::integer(),
 		      Compression::integer(),
		      Messages::list()) -> binary().
producer_request(Topic, Partition, Magic, Compression, Messages) ->
   MessagesLength = size_of_produce_messages(Messages),
   %%io:format("Messages_Length = ~w~n", [MessagesLength]),
   TopicSize = size(Topic),
   RequestSize = 2 + 2 + TopicSize + 4 + 4 + MessagesLength,

   ProducedMessages =
        lists:foldr(
          fun (Payload, Acc) ->
                  KafkaMessage = create_message(Payload, Magic, Compression),
                  <<KafkaMessage/binary, Acc/binary>>
          end,
          <<"">>,
          Messages),
    <<RequestSize:32/integer,
      ?RQ_TYPE_PRODUCE:16/integer,
      TopicSize:16/integer,
      Topic/binary,
      Partition:32/integer,
      MessagesLength:32/integer,
      ProducedMessages/binary >>.



create_message(Payload, Magic, Compression) ->
    MessageLength = 1+1+4+size(Payload),
    CheckSum = erlang:crc32(Payload),
    <<MessageLength:32/integer,
      Magic:8/integer,
      Compression:8/integer,
      CheckSum:32/integer,
      Payload/binary>>.

size_of_produce_messages(Messages) ->
    lists:foldl(fun (X, AccSize) ->
                        size(X) + AccSize + 4 + 1 + 1 + 4
                end,
                0,
                Messages).





