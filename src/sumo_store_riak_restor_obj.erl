-module(sumo_store_riak_restor_obj).

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

-export([kv_to_doc/1]).

-record(state, {conn     :: connection(),
        bucket   :: bucket(),
        index    :: index(),
        get_opts :: get_options(),
        put_opts :: put_options(),
        del_opts :: delete_options()}).

-type state() :: #state{}.

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


-spec persist(binary() | riakc_obj:riakc_obj(), sumo_internal:doc(), state()) -> 
                          sumo_store:result(sumo_internal:doc(), state()).
%% update object
%% object get from solr search, so do not have metadata
%% refetch object from riak with key and then update 
persist(<<>>, Doc, #state{conn = Conn, bucket = Bucket, put_opts = _Opts} = State) ->
  {IdField, Id} = get_id(Doc),
  JsonDoc = doc_to_json(Doc),
  ObjectData =  case riakc_pb_socket:get(Conn, Bucket, sumo_util:to_bin(Id)) of 
  {ok, OldObj} -> 
    riakc_obj:update_value(OldObj, JsonDoc);
  _ -> riakc_obj:new(Bucket, Id, JsonDoc, "application/json")
  end,
  case riakc_pb_socket:put(Conn, ObjectData, [return_body]) of
  {error, disconnected} -> 
    persist(<<>>, Doc, State);
  {error, Error} ->
    {error, Error, State};
  {ok, RespData} -> 
    Key = riakc_obj:key(RespData),
    {ok, sumo_internal:set_field(IdField, Key, Doc), State};
  _ ->
    {error, unexpected, State}
  end;

%% update object
persist(OldObj, Doc, #state{conn = Conn,  put_opts = _Opts, bucket = Bucket} = State) ->
  {IdField, Id} = get_id(Doc),
   JsonDoc = doc_to_json(Doc),

  ObjectData = 
     case riakc_obj:bucket(OldObj) of
            Bucket ->
                  riakc_obj:update_value(OldObj, JsonDoc);
            _OldBucket ->
                  riakc_obj:new(Bucket, Id, JsonDoc, "application/json")
  end,
  case riakc_pb_socket:put(Conn, ObjectData, [return_body]) of 
  {error, disconnected} -> 
     persist(OldObj, Doc, State);
  {error, Error} ->
    {error, Error, State};
  {ok, RespData} -> 
    Key = riakc_obj:key(RespData),
    {ok, sumo_internal:set_field(IdField, Key, Doc), State};
  _ ->
    {error, unexpected, State}
  end.


-spec persist(sumo_internal:doc(), state()) -> 
                          sumo_store:result(sumo_internal:doc(), state()).
%% insert object
persist(Doc,
	#state{conn = Conn, bucket = Bucket, put_opts = _Opts} = State) ->
  {IdField, Id} = get_id(Doc),
	JsonDoc = doc_to_json(Doc),
	ObjectData = riakc_obj:new(Bucket, Id, JsonDoc, "application/json"), 
	case riakc_pb_socket:put(Conn, ObjectData, [return_body]) of
  {error, disconnected} ->
    persist(Doc, State);
	{error, Error} ->
		{error, Error, State};
  {ok, RespData} -> 
    Key = riakc_obj:key(RespData),
		{ok, sumo_internal:set_field(IdField, Key, Doc), State};
  _ ->
    {error, unexpected, State}
	end.


-spec find_all(
  sumo:schema_name(), state()
) -> sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, #state{conn = Conn, bucket = Bucket, get_opts = Opts} = State) ->
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
	 find_by(DocName, [], Limit, Offset, State).


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


find_by(DocName, Conditions, SortFields, Limit, Offset,  #state{conn = Conn, index = Index} = State) ->
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


delete_by(DocName, Conditions, #state{conn = Conn, bucket = Bucket, index = Index,
		 del_opts = Opts} = State) ->
	delete_by_index(DocName, Conn, Index, Bucket, Conditions, Opts, State).


-spec delete_all(
  sumo:schema_name(), state()
) -> sumo_store:result(sumo_store:affected_rows(), state()).
delete_all(_DocName,
           #state{conn = Conn, bucket = Bucket, del_opts = Opts} = State) ->
  Del = fun(Kst, Acc) ->
    lists:foreach(fun(K) ->  riakc_pb_socket:delete(Conn, Bucket, sumo_util:to_bin(K), Opts) end, Kst),
    Acc + length(Kst)
  end,
  case stream_keys(Conn, Bucket, Del, 0) of
    {ok, Count} -> {ok, Count, State};
    {_, Count}  -> {error, Count, State}
  end.


%% private

search_by_key(DocName, Conn, Bucket, Key, Opts, State) ->
	case  riakc_pb_socket:get(Conn, Bucket, sumo_util:to_bin(Key)) of 
		{ok, FetchedData} ->
			DataJson = riakc_obj:get_value(FetchedData),
			Val = to_doc(DocName, DataJson),
			{ok, [#{doc => Val, obj => FetchedData}], State};
		{error, notfound} ->
			{ok, [], State};
		{error, disconnected} ->
			search_by_key(DocName, Conn, Bucket, Key, Opts, State);
		{error, Error} ->
			{error, Error, State}
	end. 

search_by_index(DocName, Conn, Index, Conditions, Limit, Offset, State) ->
    Query = sumo_util:build_query(Conditions),
    lager:debug("Query: ~p",[Query]),
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
      lager:debug("Query: ~p; SortQuery: ~p",[Query, SortQuery]),
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
      lager:debug("Query: ~p; SortQuery: ~p",[Query, SortQuery]),
      case search_docs_by(DocName, Conn, Index, Query, Filters, SortQuery, Limit, Offset) of 
        {ok, {_, Res}} -> {ok, Res, State} ;
        {error, Error} -> {error, Error, State}
      end
  end.


delete_by_key(Conn, Bucket, Key, Opts, State) ->
	case riakc_pb_socket:delete(Conn, Bucket, sumo_util:to_bin(Key)) of  
    ok ->
      {ok, 1, State};
    {error, disconnected} ->
      delete_by_key(Conn, Bucket, Key, Opts, State);
    {error, notfound} ->
      {ok, 0, State};
    {error, Error} ->
      {error, Error, State}
    end.

delete_by_index(DocName, Conn, Index, Bucket, Conditions, Opts, State) ->
    Query = sumo_util:build_query(Conditions),
    case riakc_pb_socket:search(Conn, Index, Query, [{start, 0}, {rows, ?LIMIT}]) of 
    {ok, {search_results, Results, _, Total}} ->
        Fun = fun({_, Obj}) ->
          Key = proplists:get_value(<<"_yz_rk">>, Obj),
          delete_by_key(Conn, Bucket,  sumo_util:to_bin(Key), Opts, State)
          %riakc_pb_socket:delete(Conn, Bucket, sumo_util:to_bin(Key), Opts)
        end, 
        lists:foreach(Fun, Results),
        {ok, Total, State};
    {error, disconnected} ->
       delete_by_index(DocName, Conn, Index, Bucket, Conditions, Opts, State);
    {error, _Error} = Err -> 
        Err
    end.



fetch_docs(DocName, Conn, Bucket, Keys, Opts) ->
	Fun = fun(K, Acc) ->
		case riakc_pb_socket:get(Conn, Bucket, K, Opts) of
		{ok, M} ->
			DataJson = riakc_obj:get_value(M),
			Val = to_doc(DocName, DataJson),
			[Val | Acc];
		_  -> Acc
		end
	end,
lists:foldl(Fun, [], Keys).


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

to_doc(DocName, DataJson) ->
  DataProps  = jsx:decode(DataJson),
  NewData = kv_to_doc(DataProps),
  sumo_internal:new_doc(DocName, NewData). 


%% @private
normalize_doc_fields(Src) ->
  re:replace(
  Src, <<"_register|_set|_counter|_flag|_map">>, <<"">>,
  [{return, binary}, global]).

kv_to_doc(KV) ->
	F = fun({K, [H|_] = V}, Acc) when is_tuple(H)  ->
          %% each element of V is tuple()
	        NK = sumo_util:to_atom(normalize_doc_fields(K)),
	       	NV = kv_to_doc(V) ,
	       	maps:put(NK, NV, Acc);
	    ({K, [ [{_SubK, _SubV} | _]| _] = V}, Acc) ->
        %% each element of V is [tuple()]
        NK = sumo_util:to_atom(normalize_doc_fields(K)),
	    	NV = lists:map(fun(VEl) -> kv_to_doc(VEl) end, V),
	    	maps:put(NK, NV, Acc);
	    ({K, V}, Acc) ->
	    	NK = sumo_util:to_atom(normalize_doc_fields(K)),
	    	maps:put(NK, V, Acc);
      (Other, Acc) ->
        lager:error("Can not decode to KV: ~p",[Other]),
        Acc
	    end,
	lists:foldl(F, #{}, KV).



%% @private
% new_doc(Doc, #state{conn = Conn, bucket = Bucket, put_opts = _Opts}) -> 
%   DocName = sumo_internal:doc_name(Doc),
%   IdField = sumo_internal:id_field_name(DocName),
%   Id = case sumo_internal:get_field(IdField, Doc) of
%   undefined ->
%   	JsonDoc = doc_to_json(Doc),
%   	ObjectData = riakc_obj:new(Bucket, undefined, JsonDoc, "application/json"), 
%   	case riakc_pb_socket:put(Conn, ObjectData) of
%   	{ok, Data} -> riakc_obj:key(Data);
%   	{error, Error} -> throw(Error);
%   	_ -> throw(unexpected)
%   	end;
%   Id0 ->
%    sumo_util:to_bin(Id0)
%   end,
%   {Id, sumo_internal:set_field(IdField, Id, Doc)}.

get_id(Doc) -> 
  DocName = sumo_internal:doc_name(Doc),
  IdField = sumo_internal:id_field_name(DocName),
  {IdField, sumo_internal:get_field(IdField, Doc)}.

doc_to_json(Doc) ->
	Fields = sumo_internal:doc_fields(Doc),
	DataProps = append_riak_suffix(Fields),
	jsx:encode(DataProps).


append_riak_suffix(Fields) ->
	lists:flatmap(fun update_field/1, maps:to_list(Fields)).

update_field({K, V}) when is_map(V) ->
	NewVal = append_riak_suffix(V),
	BinKey = sumo_util:to_bin(K),
	[{<<BinKey/binary, "_map">>, NewVal}];

update_field({K, V}) when is_list(V) ->
	BinKey = sumo_util:to_bin(K),
	case io_lib:printable_list(V) of
	true ->
		[{<<BinKey/binary, "_register">>, V}] ;
	_ ->
		{SetEls, MapEls} = map_filter(V),
		NewMaps = if MapEls /= [] ->
						[{<<BinKey/binary, "_map">>, lists:reverse(MapEls)}] ;
					true ->
						[]
				end,
		NewSet = if SetEls /= [] ->
						[{<<BinKey/binary, "_set">>, lists:reverse(SetEls)}] ;
					true ->
						[]
				end,  
		NewMaps ++ NewSet
	end; 

update_field({K, V}) ->
	BinKey = sumo_util:to_bin(K),
	case sumo_util:suffix(K) of 
	true ->  %% Key with suffix "_arr", defaut store with set
		[{<<BinKey/binary, "_set">>, V}] ;
	_ ->
		[{<<BinKey/binary, "_register">>, V}]
	end. 	

map_filter(V) ->
 lists:foldl(
 	fun(Els, {Set, Map}) when is_map(Els) ->
    	Res = lists:map(fun rset/1, maps:to_list(Els)),
    	{Set, [Res| Map]};
  	(Els, {Set, Map}) ->
    	{[Els | Set], Map}
  	end, {[], []}, V).



rset({K, V}) when is_binary(V) ->
	BinKey = sumo_util:to_bin(K),
	{<<BinKey/binary, "_set">>, V};
rset({K, V}) when is_map(V) ->
	BinKey = sumo_util:to_bin(K),
	NewVal = lists:flatmap(fun update_field/1, maps:to_list(V)),
	{<<BinKey/binary, "_map">>, NewVal};

rset({K, V}) when is_list(V) ->
  case update_field({K, V}) of
        [F] -> F;
        _ ->
        BinKey = sumo_util:to_bin(K),
                {<<BinKey/binary,"_map">>, <<>>}
end.

%% @private

search_docs_by(DocName, Conn, Index, Query, Limit, Offset) ->
 case riakc_pb_socket:search(Conn, Index, Query, [{start, Offset}, {rows, Limit}]) of
  {ok, {search_results, Results, _, Total}} ->
    F = fun({_, KV}, Acc) ->  
    {MapData, Doc} = kv_to_doc(DocName, KV),
    NewDoc = sumo_internal:new_doc(DocName, get_all_map_from_data(MapData, Doc)),
    [#{doc => NewDoc, obj => <<>>} | Acc]
    end, 
    NewRes = lists:foldl(F, [], Results),
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
      {MapData, Doc} = kv_to_doc(DocName, KV),
      NewDoc = sumo_internal:new_doc(DocName, get_all_map_from_data(MapData, Doc)),
      [#{doc => NewDoc, obj => <<>>} | Acc] 
    end, 
    NewRes = lists:foldl(F, [], Results),
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
      {MapData, Doc} = kv_to_doc(DocName, KV),
      NewDoc = sumo_internal:new_doc(DocName, get_all_map_from_data(MapData, Doc)),
      [#{doc => NewDoc, obj => <<>>} | Acc]
    end, 
    NewRes = lists:reverse(lists:foldl(F, [], Results)),
    {ok, {Total, NewRes}};
  {error, disconnected} ->
    search_docs_by(DocName, Conn, Index, Query, Filters, SortQuery, Limit, Offset);
  {error, Error} ->
    {error, Error}
  end.

% @private
%% process normal field 
%% and seperate map field to process later
kv_to_doc(_DocName, KV) ->
  F = 
  fun({K, V}, {Acc, Doc})  ->
    case lists:member(K, ?IGNORE_KEY) of 
    true ->
      {Acc, Doc} ;
    _ ->
      NK = normalize_doc_fields(K),
      case binary:split(NK, <<".">>) of 
      [NK] ->
        {Acc, update_map(sumo_util:to_atom(NK), V, Doc)}  ;
      [MainK, SubK] ->
        NewAcc = [{MainK, {SubK, V}} | Acc], 
        {NewAcc, Doc}
      end
    end
  end,
  lists:foldl(F, {[], #{}}, KV).




%% @private
update_map(K, V, Doc) ->
  case maps:get(K, Doc, none) of 
  none ->
    maps:put(K, V, Doc) ;
  [_H | _] = Values ->
    maps:put(K, Values ++ [V], Doc) ;
  Value ->
    maps:put(K, [Value, V], Doc)
  end. 

%% @private
get_all_map_from_data([], Doc) -> Doc ;
get_all_map_from_data(MapData, Doc) ->
  GetAllFirstKey = proplists:get_keys(MapData),
  lists:foldl(fun(Key, Acc) ->
    SubMapValues = proplists:get_all_values(Key, MapData),
    ArrOfMap = transform_to_arr_map(SubMapValues),
    maps:put(sumo_util:to_atom(Key), ArrOfMap, Acc)
    % sumo_internal:set_field(sumo_util:to_atom(Key), ArrOfMap, Acc)
  end, Doc, GetAllFirstKey).


transform_to_arr_map(FieldValues) -> 
  Keys = proplists:get_keys(FieldValues),
  LenKeys = length(Keys),
  LenFieldValues = length(FieldValues),
  if 
    LenKeys == LenFieldValues ->
      maps:from_list(FieldValues);
    LenKeys /= LenFieldValues -> 
      case check_symetric(Keys, FieldValues) of 
      {true, _} ->
        process_to_collect_map(FieldValues) ;
      {_, CollFields} ->
        maps:from_list(CollFields) 
      end 
  end.

check_symetric([], _FieldValues) ->
  {false, []} ;

check_symetric([Key | T], FieldValues) ->
  FirstVals= proplists:get_all_values(Key, FieldValues),
  LenVal = length(FirstVals),
  {FilterRes, NewDatas} = 
  lists:foldl(fun(K, {Acc0, Acc}) ->
    SubVs = proplists:get_all_values(K, FieldValues),
    LenV = length(SubVs),
    NewAcc0 = case LenV == LenVal of 
      true ->
        [K | Acc0] ;
      _ ->
        Acc0
    end,
    {NewAcc0, [{K, get_value(SubVs)}|Acc] }
  
  end, {[], []} , T),
  {length(FilterRes) == length(T), [{Key, get_value(FirstVals)}| NewDatas]}.


process_to_collect_map(FieldValues) -> process_to_collect_map(FieldValues, []) .

process_to_collect_map([], Acc) -> Acc; 

process_to_collect_map(FieldValues, Acc) ->
	% lager:info("FieldValues: ~p ~n",[FieldValues]),
  El = maps:from_list(lists:reverse(FieldValues)),
  RemainEls = lists:subtract(FieldValues, maps:to_list(El)),
  process_to_collect_map(RemainEls, Acc++[El]).

get_value([Val]) -> 
  Val ;
get_value(Vals) ->
  Vals.
