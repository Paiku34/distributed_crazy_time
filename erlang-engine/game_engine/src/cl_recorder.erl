%%%-------------------------------------------------------------------
%% @doc Logica Chandy-Lamport lato partecipante, in funzioni pure.
%%
%%      Nessun processo: e' lo stato del taglio, che wheel_process e
%%      worker tengono dentro il proprio #state{}. Questo e' voluto —
%%      i marker devono partire dal processo APPLICATIVO, cosi' da
%%      condividere mailbox e ordine FIFO con i messaggi che delimitano.
%%      Un marker inviato da un processo terzo non delimita nulla.
%%
%%      Il modulo non spedisce niente: apre e chiude i canali entranti,
%%      accoda i messaggi in transito e dice quando il taglio e' chiuso.
%%      L'invio dei marker uscenti resta responsabilita' del chiamante.
%% @end
%%%-------------------------------------------------------------------
-module(cl_recorder).

-record(cl, {
    id      = undefined,   %% undefined = non sta registrando
    local   = undefined,   %% stato locale salvato all'istante del taglio
    in_open = [],          %% canali entranti ancora in registrazione [{Role, Node}]
    chan    = #{}          %% #{{Role,Node} => [Msg]} messaggi in transito (invertiti)
}).

-export([new/0, start/3, is_recording/1, on_marker/5, on_app_msg/3,
         close/2, close_all/1, is_complete/1, id/1, local/1, channels/1, in_open/1]).

-export_type([cl/0]).
-type cl() :: #cl{}.

%%====================================================================
%% API
%%====================================================================

-spec new() -> cl().
new() -> #cl{}.

%% Ingresso dell'INIZIATORE del taglio: non riceve mai un marker, quindi non
%% puo' passare da on_marker/5. Salva lo stato locale e apre la registrazione
%% su tutti i canali entranti.
-spec start(term(), term(), [term()]) -> cl().
start(SnapId, LocalState, InChannels) ->
    #cl{id = SnapId, local = LocalState, in_open = InChannels, chan = #{}}.

-spec is_recording(cl()) -> boolean().
is_recording(#cl{id = undefined}) -> false;
is_recording(_) -> true.

%% Arrivo di un marker sul canale From.
%%   - primo marker: salva lo stato locale e apre tutti gli altri canali;
%%   - marker successivo: chiude il canale da cui e' arrivato.
%% Il chiamante usa l'esito per sapere se deve emettere i propri marker.
-spec on_marker(term(), term(), [term()], term(), cl()) ->
          {cl(), first_marker | subsequent}.
on_marker(SnapId, From, _InChannels, _LocalState, #cl{id = SnapId} = CL) ->
    %% Stesso taglio: e' un marker successivo, chiude il suo canale.
    {close(From, CL), subsequent};
on_marker(SnapId, From, InChannels, LocalState, _CL) ->
    %% Nessun taglio in corso, oppure un taglio nuovo che sostituisce il
    %% precedente (il vecchio non si e' chiuso: il collector era morto).
    {#cl{id = SnapId,
         local = LocalState,
         in_open = InChannels -- [From],
         chan = #{}}, first_marker}.

%% Messaggio applicativo ricevuto: entra nello stato del canale solo se
%% stiamo registrando e quel canale e' ancora aperto. Un messaggio arrivato
%% dopo il marker di quel canale appartiene gia' al taglio successivo.
-spec on_app_msg(term(), term(), cl()) -> cl().
on_app_msg(_From, _Msg, #cl{id = undefined} = CL) -> CL;
on_app_msg(From, Msg, #cl{in_open = Open, chan = Chan} = CL) ->
    case lists:member(From, Open) of
        false -> CL;
        true  ->
            Prev = maps:get(From, Chan, []),
            CL#cl{chan = maps:put(From, [Msg | Prev], Chan)}
    end.

-spec close(term(), cl()) -> cl().
close(From, #cl{in_open = Open} = CL) ->
    CL#cl{in_open = lists:delete(From, Open)}.

%% Chiude in blocco cio' che resta aperto: percorso di abort, quando il
%% collector non risponde e il partecipante non puo' restare in attesa.
-spec close_all(cl()) -> cl().
close_all(CL) -> CL#cl{in_open = []}.

-spec is_complete(cl()) -> boolean().
is_complete(#cl{id = undefined}) -> false;
is_complete(#cl{in_open = []}) -> true;
is_complete(_) -> false.

-spec id(cl()) -> term().
id(#cl{id = Id}) -> Id.

-spec local(cl()) -> term().
local(#cl{local = L}) -> L.

%% Canali entranti ancora in registrazione. Il chiamante ne ha bisogno per
%% sapere se un messaggio in arrivo appartiene al taglio o al round nuovo.
-spec in_open(cl()) -> [term()].
in_open(#cl{in_open = Open}) -> Open.

%% Stati dei canali, con i messaggi nell'ordine in cui sono arrivati.
-spec channels(cl()) -> #{term() => [term()]}.
channels(#cl{chan = Chan}) ->
    maps:map(fun(_K, V) -> lists:reverse(V) end, Chan).
