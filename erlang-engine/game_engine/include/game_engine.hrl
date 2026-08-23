%%%-------------------------------------------------------------------
%% @doc Definizioni condivise fra i moduli dell'engine.
%% @end
%%%-------------------------------------------------------------------

%% Checkpoint di un round, prodotto dallo snapshot Chandy-Lamport e
%% replicato su Mnesia (disc_copies) su tutti i nodi del cluster.
%%
%% E' l'artefatto che rende lo snapshot utile a qualcosa: da qui un nuovo
%% leader eletto dopo un crash puo' COMPLETARE il round interrotto invece
%% di annullarlo, e da qui si ricostruisce l'insieme delle bet gia'
%% liquidate (deduplica cross-round).
%%
%% La chiave e' {Round, Initiator} e non un contatore: due leader
%% concorrenti (split-brain non contenuto) producono record distinti e
%% diagnosticabili invece di sovrascriversi in silenzio.
-record(snapshot_record, {
    id,               %% {Round :: integer(), Initiator :: node()}  — chiave
    round,            %% integer()
    taken_at,         %% erlang:system_time(millisecond)
    initiator,        %% node() che ha avviato il taglio
    degraded,         %% boolean() — taglio chiuso per timeout, partecipanti mancanti
    phase,            %% fase del round al momento del taglio
    winner_segment,   %% esito catturato insieme alle bet
    winner_index,
    local_states,     %% #{Participant => term()}
    channel_states,   %% #{{From, To} => [term()]} messaggi in transito
    ledger            %% [BetMap] insieme autorevole delle bet del round
}).
