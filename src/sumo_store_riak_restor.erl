-module(sumo_store_riak_restor).

-behaviour(sumo_store).

-include("sumo.hrl").
-include_lib("riakc/include/riakc.hrl").


-export([init/1
        ,create_schema/2
        ,persist/2
        ,persist/3
        ,find_all/2
        ,find_all/5
        ,find_by/3
        ,find_by/5
        ,find_by/6
        ,find_by/7
        ,delete_by/3
        ,delete_all/2]).

-export([search_by_index/7
        ,delete_by_index/7]).

-record(state, {conn     :: connection(),
        bucket   :: bucket(),
        index    :: index(),
        get_opts :: get_options(),
        put_opts :: put_options(),
        del_opts :: delete_options()}).

-type state() :: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% conn: is the Pid of the gen_server that holds the connection with Riak
%% bucket: Riak bucket (per store)
%% index: Riak index to be used by Riak Search
%% read_quorum: Riak read quorum parameters.
%% write_quorum: Riak write quorum parameters.
%% @see <a href="http://docs.basho.com/riak/latest/dev/using/basics"/>

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec create_schema(
  sumo:schema(), state()
) -> sumo_store:result(state()).
create_schema(_Schema, State) ->
  {ok, State}.

-spec init(
  term()
) -> {ok, term()}.
init(Opts) ->
  % The storage backend key in the options specifies the name of the process
  % which creates and initializes the storage backend.
  % Backend = proplists:get_value(storage_backend, Opts),
  % Conn = sumo_backend_riak:get_connection(Backend),
  BucketType = iolist_to_binary(
    proplists:get_value(bucket_type, Opts, <<"maps">>)),
  Bucket = iolist_to_binary(
    proplists:get_value(bucket, Opts, <<"sumo">>)),
  Index = iolist_to_binary(
    proplists:get_value(index, Opts, <<"sumo_index">>)),
  GetOpts = proplists:get_value(get_options, Opts, []),
  PutOpts = proplists:get_value(put_options, Opts, []),
  DelOpts = proplists:get_value(delete_options, Opts, []),
  State = #state{%conn = Conn,
                 bucket = {BucketType, Bucket},
                 index = Index,
                 get_opts = GetOpts,
                 put_opts = PutOpts,
                 del_opts = DelOpts},
  {ok, State}.


-spec persist(binary() | riakc_map:crdt_map(), sumo_internal:doc(), state()) -> 
                          sumo_store:result(sumo_internal:doc(), state()).

%% update object
%% object get from solr search, so do not have metadata
%% refetch object from riak with key and then update 
persist(<<>>, Doc, #state{conn = Conn, bucket = Bucket, put_opts = Opts} = State) ->
  {Id, NewDoc} = new_doc(Doc, State),
  InitObj = case fetch_map(Conn, Bucket, sumo_util:to_bin(Id), Opts) of 
  {ok, OldObj} -> OldObj;
  _ -> riakc_map:new()
  end,
  DocRMap = doc_to_rmap(NewDoc, InitObj),
  case update_map(Conn, Bucket, Id, DocRMap, Opts) of 
  {error, disconnected} -> 
    persist(<<>>, Doc, State);
  {error, Error} ->
    {error, Error, State};
  _ ->
    {ok, NewDoc, State}
  end;

%% update object which fetch from riak
persist(OldObj, Doc, #state{conn = Conn, bucket = Bucket,  put_opts = Opts} = State) ->
  {Id, NewDoc} = new_doc(Doc, State),
  DocRMap = doc_to_rmap(NewDoc, OldObj),
  case update_map(Conn, Bucket, Id, DocRMap , Opts) of 
    {error, disconnected} -> 
      persist(OldObj, Doc, State);
    {error, Error} ->
      {error, Error, State};
    _ ->
      {ok, NewDoc, State}
  end.

-spec persist(
  sumo_internal:doc(), state()
) -> sumo_store:result(sumo_internal:doc(), state()).
%% insert object
persist(Doc, #state{conn = Conn, bucket = Bucket, put_opts = Opts} = State) ->
  {Id, NewDoc} = new_doc(Doc, State),
  DocRMap = doc_to_rmap(NewDoc, riakc_map:new()),
  case update_map(Conn, Bucket, Id, DocRMap, Opts) of
    {error, disconnected} ->
      persist(Doc, State);
    {error, Error} ->
      {error, Error, State};
    _ ->
      {ok, NewDoc, State}
  end.

-spec find_all(
  sumo:schema_name(), state()
) -> sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName,
         #state{conn = Conn, bucket = Bucket, get_opts = Opts} = State) ->
  Get = fun(Kst, Acc) ->
          fetch_docs(DocName, Conn, Bucket, Kst, Opts) ++ Acc
        end,
  case stream_keys(Conn, Bucket, Get, []) of
    {ok, Docs} -> {ok, Docs, State};
    {_, Docs}  -> {error, Docs, State}
  end.


-spec find_all(
  sumo:schema_name(),
  term(),
  non_neg_integer(),
  non_neg_integer(),
  state()
) -> sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, _SortFields, Limit, Offset, State) ->
%% @todo implement search with sort parameters.
  find_by(DocName, [], Limit, Offset, State).



%% find_by may be used in two ways: either with a given limit and offset or not
%% If a limit and offset is not given, then the atom 'undefined' is used as a
%% marker to indicate that the store should find out how many keys matching the
%% query exist, and then obtain results for all of them.
%% This is done to overcome Solr's defaulta pagination value of 10.

find_by(DocName, Conditions, State) ->
  find_by(DocName, Conditions, ?LIMIT, 0, State).


find_by(DocName, Conditions, Limit, Offset,
    #state{conn = Conn,
         bucket = Bucket,
         index = Index,
         get_opts = Opts} = State) when is_list(Conditions) ->

  IdField = sumo_internal:id_field_name(DocName),
  %% If the key field is present in the conditions, we are looking for a
  %% particular document. If not, it is a general query.
  case Conditions of 
  [{IdField, Key}] -> 
    search_by_key(DocName, Conn, Bucket, Key, Opts, State) ;
  _ ->
    search_by_index(DocName, Conn, Index, Conditions, Limit, Offset, State)
  end;

 
find_by(DocName, Conditions, Limit, Offset, #state{conn = Conn, index = Index} = State) ->

    search_by_index(DocName, Conn, Index, Conditions, Limit, Offset, State).
 

find_by(DocName, Conditions, SortFields, Limit, Offset, #state{conn = Conn, index = Index} = State) ->

  search_by_index(DocName, Conn, Index, Conditions, SortFields, Limit, Offset, State).


find_by(DocName, Conditions, Filters, SortFields, Limit, Offset, #state{conn = Conn, index = Index} = State) ->

    search_by_index(DocName, Conn, Index, Conditions, Filters, SortFields, Limit, Offset, State).
 

-spec delete_by(
  sumo:schema_name(), sumo:conditions(), state()
) -> sumo_store:result(sumo_store:affected_rows(), state()).
delete_by(DocName, Conditions,
      #state{conn = Conn,
         bucket = Bucket,
         index = Index,
         del_opts = Opts} = State) when is_list(Conditions) ->
  IdField = sumo_internal:id_field_name(DocName),
  case Conditions of 
  [{IdField, Key}] -> 

    delete_by_key(Conn, Bucket, Key, Opts, State);

  _ ->

    delete_by_index(DocName, Conn, Index, Bucket, Conditions, Opts, State)
  end;

delete_by(DocName, Conditions,
      #state{conn = Conn,
         bucket = Bucket,
         index = Index,
         del_opts = Opts} = State) ->
    
    delete_by_index(DocName, Conn, Index, Bucket, Conditions, Opts, State).

-spec delete_all(
  sumo:schema_name(), state()
) -> sumo_store:result(sumo_store:affected_rows(), state()).
delete_all(_DocName,
           #state{conn = Conn, bucket = Bucket, del_opts = Opts} = State) ->
  Del = fun(Kst, Acc) ->
          lists:foreach(fun(K) -> delete_map(Conn, Bucket, K, Opts) end, Kst),
          Acc + length(Kst)
        end,
  case stream_keys(Conn, Bucket, Del, 0) of
    {ok, Count} -> {ok, Count, State};
    {_, Count}  -> {error, Count, State}
  end.

%% public 

search_by_key(DocName, Conn, Bucket, Key, Opts, State) ->
   case fetch_map(Conn, Bucket, sumo_util:to_bin(Key), Opts) of 
    {ok, RMap} ->
      Val = rmap_to_doc(DocName, RMap),
      % {ok, [Val], State};
      {ok, [#{doc => Val, obj => RMap}], State};
    {error, {notfound, _}} ->
      {ok, [], State};
    {error, disconnected} ->
      search_by_key(DocName, Conn, Bucket, Key, Opts, State);
    {error, Error} ->
      {error, Error, State}
    end.

search_by_index(DocName, Conn, Index, Conditions, Limit, Offset, State) ->
    Query = sumo_util:build_query(Conditions),
    case Query of 
      ?INVALID_QUERY ->
        {ok, [], State} ;
      _ -> 
      case search_docs_by(DocName, Conn, Index, Query, Limit, Offset) of
      {ok, {_, Res}} -> {ok, Res, State};
      {error, Error} -> {error, Error, State}
      end
    end.

search_by_index(DocName, Conn, Index, Conditions, SortFields, Limit, Offset, State) ->
  Query = sumo_util:build_query(Conditions),
  case Query of 
    ?INVALID_QUERY ->
      {ok, [], State} ;
    _ -> 
      SortQuery = sumo_util:build_sort_query(SortFields),
      case search_docs_by(DocName, Conn, Index, Query, SortQuery, Limit, Offset) of 
        {ok, {_, Res}} -> {ok, Res, State} ;
        {error, Error} -> {error, Error, State}
      end
  end. 

search_by_index(DocName, Conn, Index, Conditions, Filters, SortFields, Limit, Offset, State) ->
  Query = sumo_util:build_query(Conditions),
  case Query of 
    ?INVALID_QUERY ->
      {ok, [], State} ;
    _ -> 
      SortQuery = sumo_util:build_sort_query(SortFields),
      case search_docs_by(DocName, Conn, Index, Query, Filters, SortQuery, Limit, Offset) of 
        {ok, {_, Res}} -> {ok, Res, State} ;
        {error, Error} -> {error, Error, State}
      end
  end.

delete_by_key(Conn, Bucket, Key, Opts, State) -> 
   case delete_map(Conn, Bucket, sumo_util:to_bin(Key), Opts) of 
    ok ->
      {ok, 1, State};
    {error, disconnected} ->
      delete_by_key(Conn, Bucket, Key, Opts, State);
    {error, notfound} -> 
      {ok, 0, State};
    {error, Error} ->
      {error, Error, State}
    end.


delete_by_index(_DocName, Conn, Index, Bucket, Conditions, Opts, State) ->
    Query = sumo_util:build_query(Conditions),
    case riakc_pb_socket:search(Conn, Index, Query, [{start, 0}, {rows, ?LIMIT}]) of 
    {ok, {search_results, Results, _, Total}} ->
        Fun = fun({_, Obj}) ->
          Key = proplists:get_value(<<"_yz_rk">>, Obj),
          delete_by_key(Conn, Bucket, sumo_util:to_bin(Key), Opts, State)
          %%riakc_pb_socket:delete(Conn, Bucket, sumo_util:to_bin(Key), Opts)
        end, 
        lists:foreach(Fun, Results),
        {ok, Total, State};
    {error, disconnected} ->
        delete_by_index(_DocName, Conn, Index, Bucket, Conditions, Opts, State);
    {error, _Error} = Err -> 
        Err
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% utility 
-spec doc_to_rmap(sumo_internal:doc(), riakc_map:crdt_map()) -> riakc_map:crdt_map().
doc_to_rmap(Doc, InitObjMap) ->
  Fields = sumo_internal:doc_fields(Doc),
  map_to_rmap(Fields, InitObjMap).

-spec map_to_rmap(map(), riakc_map:crdt_map()) -> riakc_map:crdt_map().
map_to_rmap(Map, InitObjMap) ->
  % RMap = lists:foldl(fun rmap_update/2, riakc_map:new(), maps:to_list(Map)),
  lists:foldl(fun rmap_update/2, InitObjMap, maps:to_list(Map)).

-spec map_to_rmap(map()) -> riakc_map:crdt_map().
map_to_rmap(Map) ->
  lists:foldl(fun rmap_update/2, riakc_map:new(), maps:to_list(Map)).



rmap_to_doc(DocName, RMap) ->
  sumo_internal:new_doc(DocName, rmap_to_map(RMap)).

-spec rmap_to_map(riakc_map:crdt_map()) -> map().
rmap_to_map(RMap) ->
  F = fun({{K, map}, V}, Acc) ->
        maps:put(sumo_util:to_atom(K), rmap_to_map({map, V, [], [], undefined}), Acc);
      ({{K, _}, V}, Acc) ->
        maps:put(sumo_util:to_atom(K), V, Acc)
      end,
  lists:foldl(F, #{}, riakc_map:value(RMap)).

%% @private

fetch_docs(DocName, Conn, Bucket, Keys, Opts) ->
  Fun = fun(K, Acc) ->
          case fetch_map(Conn, Bucket, K, Opts) of 
            {ok, M} ->
              [rmap_to_doc(DocName, M) | Acc];
            _ -> Acc
          end
        end,
  lists:foldl(Fun, [], Keys).


%% @private
list_to_rset(_, [], Acc) ->
  Acc;
list_to_rset(KeySet, [H | T], Acc) ->
  M = riakc_map:update(KeySet, fun(S) -> riakc_set:add_element(sumo_util:to_bin(H), S) 
  end, Acc),
  list_to_rset(KeySet, T, M).

delete_key_rset(_, [], Acc) ->
  Acc;
delete_key_rset(KeySet, [H | T], Acc) ->
  M = riakc_map:update(KeySet, fun(S) -> riakc_set:del_element(sumo_util:to_bin(H), S) 
  end, Acc),
  delete_key_rset(KeySet, T, M).

get_key_rset_oldvalues(KeySet, RMap) ->
  case riakc_map:find(KeySet, RMap) of 
  {ok, OldValue} when is_binary(OldValue) ->
      [OldValue] ;
  {ok, OldValues} when is_list(OldValues) ->
      OldValues;
  _ -> []
  end.

%% @private
rmap_update({K, V}, RMap) when is_map(V) ->
  NewV = map_to_rmap(V),
  riakc_map:update({sumo_util:to_bin(K), map}, fun(_M) -> NewV end, RMap);
rmap_update({K, V}, RMap) when is_list(V) ->
  case io_lib:printable_list(V) of
    true ->
      riakc_map:update(
        {sumo_util:to_bin(K), register},
        fun(R) -> riakc_register:set(sumo_util:to_bin(V), R) end,
        RMap);
    false ->
      KeySet = {sumo_util:to_bin(K), set},
      OldValues = get_key_rset_oldvalues(KeySet, RMap),
      RemoveValues = lists:subtract(OldValues, V),
      NewRMap = delete_key_rset(KeySet, RemoveValues, RMap),
      list_to_rset(KeySet,  lists:reverse(V), NewRMap)
  end;

rmap_update({K, V}, RMap) ->
  case sumo_util:suffix(K) of 
  true ->  %% Key with suffix "_arr", defaut store with set
    NewV = case is_binary(V) of 
      true -> [V] ;
      _ -> V 
    end,
    KeySet = {sumo_util:to_bin(K), set},
    OldValues = get_key_rset_oldvalues(KeySet, RMap),
    RemoveValues = lists:subtract(OldValues, NewV),
    NewRMap = delete_key_rset(KeySet, RemoveValues, RMap),
    list_to_rset(KeySet, NewV, NewRMap);
  _ ->
    riakc_map:update(
    {sumo_util:to_bin(K), register},
    fun(R) -> riakc_register:set(sumo_util:to_bin(V), R) end,
    RMap)
  end. 

%% @private

search_docs_by(DocName, Conn, Index, Query, Limit, Offset) ->
 case riakc_pb_socket:search(Conn, Index, Query, [{start, Offset}, {rows, Limit}]) of
    {ok, {search_results, Results, _, Total}} ->
      F = fun({_, KV}, Acc) -> 
        NewDoc = kv_to_doc(DocName, KV),
        [#{doc => NewDoc, obj => <<>>} | Acc] 
      end,
      NewRes = lists:reverse(lists:foldl(F, [], Results)),
      {ok, {Total, NewRes}};
    {error, disconnected} ->
      search_docs_by(DocName, Conn, Index, Query, Limit, Offset);
    {error, Error} ->
      {error, Error}
  end.


search_docs_by(DocName, Conn, Index, Query, SortQuery, Limit, Offset) ->
  case riakc_pb_socket:search(Conn, Index, Query, [{start, Offset}, {rows, Limit}, {sort, SortQuery}]) of
    {ok, {search_results, Results, _, Total}} ->
      F = fun({_, KV}, Acc) -> 
        NewDoc = kv_to_doc(DocName, KV),
        [#{doc => NewDoc, obj => <<>>} | Acc] 
      end,
      NewRes = lists:reverse(lists:foldl(F, [], Results)),
      {ok, {Total, NewRes}};
    {error, disconnected} ->
      search_docs_by(DocName, Conn, Index, Query, SortQuery, Limit, Offset);
    {error, Error} ->
      {error, Error}
  end.

search_docs_by(DocName, Conn, Index, Query, Filters, SortQuery, Limit, Offset) ->
  case riakc_pb_socket:search(Conn, Index, Query, [{filter, Filters}, {start, Offset}, {rows, Limit}, {sort, SortQuery}]) of
      {ok, {search_results, Results, _, Total}} ->
      F = fun({_, KV}, Acc) -> 
        NewDoc = kv_to_doc(DocName, KV),
        [#{doc => NewDoc, obj => <<>>} | Acc] 
      end,
      NewRes = lists:reverse(lists:foldl(F, [], Results)),
      {ok, {Total, NewRes}};
    {error, disconnected} ->
      search_docs_by(DocName, Conn, Index, Query, Filters, SortQuery, Limit, Offset);
    {error, Error} ->
      {error, Error}
  end.


-spec update_map(
  connection(), bucket(), key() | undefined, riakc_map:crdt_map(), options()
) ->
  ok | {ok, Key::binary()} | {ok, riakc_datatype:datatype()} |
  {ok, Key::binary(), riakc_datatype:datatype()} | {error, term()}.
update_map(Conn, Bucket, Key, Map, Opts) ->
  riakc_pb_socket:update_type(Conn, Bucket, Key, riakc_map:to_op(Map), Opts).


-spec fetch_map(
  connection(), bucket(), key(), options()
) -> {ok, riakc_datatype:datatype()} | {error, term()}.
fetch_map(Conn, Bucket, Key, Opts) ->
  riakc_pb_socket:fetch_type(Conn, Bucket, Key, Opts).


-spec delete_map(
  connection(), bucket(), key(), options()
) -> ok | {error, term()}.
delete_map(Conn, Bucket, Key, Opts) ->
  riakc_pb_socket:delete(Conn, Bucket, Key, Opts).


kv_to_doc(DocName, KV) ->
  % lager:info("KV: ~p ~n",[KV]),
  F = fun({K, V}, {Acc, AccSet, AccMap}) ->
        case lists:member(K, ?IGNORE_KEY) of 
          true -> 
            {Acc, AccSet, AccMap};
          _ ->
            NK = normalize_doc_fields(K),
            case binary:split(NK, <<".">>) of 
              [MainK, SubK] ->
                NewAccMap = [{MainK, {SubK, V}} | AccMap],
                {Acc, AccSet, NewAccMap};
              _ ->
                case binary:match(K, <<"_set">>) of 
                nomatch ->
                  NewAcc = sumo_internal:set_field(sumo_util:to_atom(NK), V, Acc),
                  {NewAcc, AccSet, AccMap};
                _ ->
                  NewAccSet = [{NK, V} | AccSet],
                  {Acc, NewAccSet, AccMap}
                end 
            end 
        end
      end, 
  {Doc, SetVals, MapVals} = lists:foldl(F, {sumo_internal:new_doc(DocName), [], []}, KV), 
  % lager:info("Doc: ~p ~n; SetVals: ~p ~n; MapVals: ~p ~n",[Doc, SetVals, MapVals]),
  NewDoc = collect_set(Doc, SetVals),
  % lager:info("NewDoc: ~p ~n",[NewDoc]),
  collect_map(NewDoc, MapVals).


collect_set(Doc, SetVals) ->
  Keys = proplists:get_keys(SetVals),
  lists:foldl(fun(K, Acc) ->
    Vals = proplists:get_all_values(K, SetVals),
    sumo_internal:set_field(sumo_util:to_atom(K), Vals, Acc)
  end, Doc, Keys).

collect_map(Doc, MapVals) ->
  Keys = proplists:get_keys(MapVals),
  lists:foldl(fun(K, Acc) ->
    Vals = proplists:get_all_values(K, MapVals),
    MapEl = maps:from_list(Vals),
    sumo_internal:set_field(sumo_util:to_atom(K), MapEl, Acc)
  end, Doc, Keys).
%% @private
stream_keys(Conn, Bucket, F, Acc) ->
  % {ok, Ref} = riakc_pb_socket:get_index_eq(
  %   Conn, Bucket, <<"$bucket">>, <<"">>, [{stream, true}]),
  {ok, Ref} = riakc_pb_socket:stream_list_keys(Conn, Bucket),
  receive_stream(Ref, F, Acc).

%% @private
receive_stream(Ref, F, Acc) ->
  receive
    {Ref, {_, Stream}} ->
      receive_stream(Ref, F, F(Stream, Acc));
    {Ref, done} -> 
      {ok, Acc};
    Error ->
      lager:error("sumo_db: stream_keys error: ~p",[Error]),
      {error, Acc}
  after
    30000 -> {timeout, Acc}
  end.


%% @private
normalize_doc_fields(Src) ->
  re:replace(
  Src, <<"_register|_set|_counter|_flag|_map">>, <<"">>,
  [{return, binary}, global]).


%% @private
% delete_docs(Conn, Bucket, Docs, Opts) ->
%   F = fun(D) ->
%     K = doc_id(D),
%     % sumo_store_riak:delete_map(Conn, Bucket, K, Opts)
%     delete_map(Conn, Bucket, K, Opts)
%     end,
%   lists:foreach(F, Docs).

%% @private
% doc_id(Doc) ->
%   DocName = sumo_internal:doc_name(Doc),
%   IdField = sumo_internal:id_field_name(DocName),
%   sumo_internal:get_field(IdField, Doc).
  


% get_value([Val]) -> 
%   Val ;
% get_value(Vals) ->
%   Vals.


% @private
new_doc(Doc, #state{conn = Conn, bucket = Bucket, put_opts = Opts}) ->
  DocName = sumo_internal:doc_name(Doc),
  IdField = sumo_internal:id_field_name(DocName),
  Id = case sumo_internal:get_field(IdField, Doc) of
     undefined ->
       % case sumo_store_riak:update_map(Conn, Bucket, undefined, doc_to_rmap(Doc), Opts) of
      case  update_map(Conn, Bucket, undefined, doc_to_rmap(Doc, riakc_map:new()), Opts) of 
       {ok, RiakMapId} -> RiakMapId;
       {error, Error}  -> throw(Error);
       _               -> throw(unexpected)
       end;
     Id0 ->
       sumo_util:to_bin(Id0)
     end,
  {Id, sumo_internal:set_field(IdField, Id, Doc)}.


ts() ->
  {Mega, Sec, Micro} = os:timestamp(),
  trunc((Mega * 100000 * 100000  +  Sec * 1000000 + Micro)/1000). 
