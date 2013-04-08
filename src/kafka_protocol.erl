-module(kafka_protocol).

%% Some ideas are taken from
%% https://github.com/wooga/kafka-erlang.git


-export([producer_request/2]).


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
    %%io:format("Messages_Length = ~w~n", [MessagesLength]),
    ProducedMessages =
        lists:foldr(
          fun (Payload, Acc) ->
                  KafkaMessage = create_message(Payload, Magic, Compression),
                  <<KafkaMessage/binary, Acc/binary>>
          end,
          <<"">>,
          Messages),
    MessagesLength = size(ProducedMessages),
    TopicSize = size(Topic),
    ClientID = <<"client1">>,
    CLientIdSize = size(ClientID),
    RequestSize = 2 + 2 + 4 + 2 + CLientIdSize +
        2 + 4 + 4+ 2 + TopicSize +
        4 + 4 + 4 + MessagesLength,

    <<
      %% 1 + 4 ful-fält
      RequestSize:32/signed-integer,

      %% This is the headder for one request message
      0:16/signed-integer, % API key for preduce request = 0
      0:16/signed-integer, % API version
      0:32/signed-integer, % CorreleationID
      CLientIdSize:16/signed-integer, % ClientID size of string
      ClientID/binary,        % ClientID

      %% This is the produceRequest = one request message
      0:16/signed-integer, % RequiredAccs
      0:32/signed-integer, % Timeout
      1:32/signed-integer, % 1 hardcoded "topic array"
      TopicSize:16/signed-integer, %Size of the Topic string
      Topic/binary,                % Topic string
      1:32/signed-integer, % hardcoded 1 "partition+messageset array"
      Partition:32/signed-integer,
      MessagesLength:32/signed-integer,
      ProducedMessages/binary
    >>.



create_message(Payload, Magic, Compression) ->
    PayloadSize = size(Payload),
    KeyBytes = <<"">>, %hardcoded empty key
    KeyByteSize = size(KeyBytes),

    MessageBinaryNoCheckSum =
        <<
          Magic:8/signed-integer,
          Compression:8/signed-integer, % Attributes = Compression ?
          KeyByteSize:32/signed-integer, %length of the Key byte array
          KeyBytes/binary,
          PayloadSize:32/signed-integer, %length of the Value byte array
          Payload/binary>>,

    CheckSum = erlang:crc32(MessageBinaryNoCheckSum),
    MessageBinary =
        <<CheckSum:32/integer,
          MessageBinaryNoCheckSum/binary>>,
    MessageLength =  size(MessageBinary),
    <<0:64/signed-integer, %Offset = 0
      MessageLength:32/signed-integer,
      MessageBinary/binary>>.







