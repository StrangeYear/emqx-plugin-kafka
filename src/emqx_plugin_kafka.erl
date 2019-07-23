%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

-include_lib("emqx/include/emqx.hrl").

-define(APP, emqx_plugin_kafka).

-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/4, on_client_disconnected/3]).

-export([on_client_subscribe/3, on_client_unsubscribe/3]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/3]).

-export([on_message_publish/2, on_message_delivered/3, on_message_acked/3]).

%% Called when the plugin application start
load(Env) ->
  ekaf_init([Env]),
  emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
  emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
  emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
  emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
  emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
  emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
  emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
  emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
  emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
  emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
  emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]),
  io:format("load completed~n", []).

%%--------------------------------------------------------------------
%% Client connected
%%--------------------------------------------------------------------

on_client_connected(#{client_id := ClientId, username := Username}, 0, ConnInfo, _Env) ->
  {IpAddr, _Port} = maps:get(peername, ConnInfo),
  Params = [{action, client_connected},
              {client_id, ClientId},
              {username, Username},
              {keepalive, maps:get(keepalive, ConnInfo)},
              {ipaddress, iolist_to_binary(ntoa(IpAddr))},
              {proto_ver, maps:get(proto_ver, ConnInfo)},
              {connected_at, emqx_time:now_secs(maps:get(connected_at, ConnInfo))},
              {conn_ack, 0}],
  ekaf_send(<<"connected">>, Params),
  ok;

on_client_connected(#{}, _ConnAck, _ConnInfo, _Env) ->
    ok.

%%--------------------------------------------------------------------
%% Client disconnected
%%--------------------------------------------------------------------

on_client_disconnected(#{}, auth_failure, _Env) ->
    ok;

on_client_disconnected(Client, {shutdown, Reason}, Env) when is_atom(Reason) ->
    on_client_disconnected(Reason, Client, Env);

on_client_disconnected(#{client_id := ClientId, username := Username}, Reason, _Env)
    when is_atom(Reason) ->
    Params = [{action, client_disconnected},
              {client_id, ClientId},
              {username, Username},
              {reason, Reason}],
    ekaf_send(<<"disconnected">>, Params),
    ok;

on_client_disconnected(_, Reason, _Env) ->
    ok.

%%--------------------------------------------------------------------
%% Client subscribe
%%--------------------------------------------------------------------

on_client_subscribe(#{client_id := ClientId, username := Username}, TopicTable, {Filter}) ->
  {ok, TopicTable}.

%%--------------------------------------------------------------------
%% Client unsubscribe
%%--------------------------------------------------------------------

on_client_unsubscribe(#{client_id := ClientId, username := Username}, TopicTable, {Filter}) ->
  {ok, TopicTable}.

%%--------------------------------------------------------------------
%% Session created
%%--------------------------------------------------------------------

on_session_created(#{client_id := ClientId}, SessInfo, _Env) ->
  io:format("session(~s) created.", [ClientId]),
  ok.

%%--------------------------------------------------------------------
%% Session subscribed
%%--------------------------------------------------------------------

on_session_subscribed(#{client_id := ClientId, username := Username}, Topic, Opts, {Filter}) ->
  io:format("session(~s/~s) subscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
  Params = [{action, session_subscribed},
                  {client_id, ClientId},
                  {username, Username},
                  {topic, Topic},
                  {opts, Opts}],
  ekaf_send(<<"subscribed">>, Params),
  {ok, {Topic, Opts}}.

%%--------------------------------------------------------------------
%% Session unsubscribed
%%--------------------------------------------------------------------

on_session_unsubscribed(#{client_id := ClientId}, Topic, Opts, {Filter}) ->
  io:format("session(~s/~s) unsubscribed: ~p~n", [ClientId, {Topic, Opts}]),
  ok.

%%--------------------------------------------------------------------
%% Session terminated
%%--------------------------------------------------------------------

on_session_terminated(Info, {shutdown, Reason}, Env) when is_atom(Reason) ->
    on_session_terminated(Info, Reason, Env);

on_session_terminated(#{client_id := ClientId}, Reason, _Env) when is_atom(Reason) ->
    io:format("session(~s) terminated: ~p.", [ClientId, Reason]),
    ok;

on_session_terminated(#{}, Reason, _Env) ->
    ok.

%%--------------------------------------------------------------------
%% Message publish
%%--------------------------------------------------------------------

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{topic = Topic, flags = #{retain := Retain}}, {Filter}) ->
        {FromClientId, FromUsername} = format_from(Message),
        Params = [{action, message_publish},
                  {from_client_id, FromClientId},
                  {from_username, FromUsername},
                  {topic, Message#message.topic},
                  {qos, Message#message.qos},
                  {retain, Retain},
                  {payload, Message#message.payload},
                  {ts, emqx_time:now_secs(Message#message.timestamp)}],
  ekaf_send(<<"publish">>, Params),
  {ok, Message}.

%%--------------------------------------------------------------------
%% Message deliver
%%--------------------------------------------------------------------

on_message_delivered(#{client_id := ClientId, username := Username}, Message = #message{topic = Topic, flags = #{retain := Retain}}, {Filter}) ->
      {FromClientId, FromUsername} = format_from(Message),
      Params = [{action, message_deliver},
                {client_id, ClientId},
                {username, Username},
                {from_client_id, FromClientId},
                {from_username, FromUsername},
                {topic, Message#message.topic},
                {qos, Message#message.qos},
                {retain, Retain},
                {payload, Message#message.payload},
                {ts, emqx_time:now_secs(Message#message.timestamp)}],
  ekaf_send(<<"public">>, Params),
  {ok, Message}.

%%--------------------------------------------------------------------
%% Message acked
%%--------------------------------------------------------------------

on_message_acked(#{client_id := ClientId}, Message = #message{topic = Topic, flags = #{retain := Retain}}, {Filter}) ->
  {ok, Message}.

%% Called when the plugin application stop
unload() ->
  emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
  emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
  emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
  emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
  emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
  emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
  emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
  emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
  emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
  emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
  emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3).


%% ==================== ekaf_init STA.===============================%%
ekaf_init(_Env) ->
  % clique 方式读取配置文件
  {ok, Env} = application:get_env(?APP, kafka),
  Host = proplists:get_value(host, Env),
  Port = proplists:get_value(port, Env),
  Topic = proplists:get_value(topic, Env),
  io:format("~w ~w ~w ~n", [Host, Port, Topic]),

  % init kafka
  application:load(ekaf),
  application:set_env(ekaf, ekaf_bootstrap_broker, {Host, Port}),
  application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
  application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
  application:set_env(ekaf, ekaf_buffer_ttl, 100),

  {ok, _} = application:ensure_all_started(ekaf),

  io:format("Init ekaf with ~s:~b~n", [Host, Port]).
%% ==================== ekaf_init END.===============================%%

%% ==================== ekaf_send STA.===============================%%
ekaf_send(Type, Params) ->
  Json = jsx:encode(Params),
  ekaf_send_sync(Json).

ekaf_send_sync(Msg) ->
  Topic = ekaf_get_topic(),
  ekaf_send_sync(Topic, Msg).
ekaf_send_sync(Topic, Msg) ->
  ekaf:produce_sync_batched(list_to_binary(Topic), list_to_binary(Msg)).

format_from(#message{from = ClientId, headers = #{username := Username}}) ->
    {a2b(ClientId), a2b(Username)};
format_from(#message{from = ClientId, headers = _HeadersNoUsername}) ->
    {a2b(ClientId), <<"undefined">>}.

a2b(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8);
a2b(A) -> A.

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

%% ==================== ekaf_send END.===============================%%

%% ==================== ekaf_set_topic STA.===============================%%
ekaf_set_topic(Topic) ->
  application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
  ok.
ekaf_get_topic() ->
  Env = application:get_env(?APP, kafka),
  Topic = proplists:get_value(topic, Env),
  Topic.
%% ==================== ekaf_set_topic END.===============================%%