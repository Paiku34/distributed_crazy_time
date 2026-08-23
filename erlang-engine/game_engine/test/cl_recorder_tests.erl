-module(cl_recorder_tests).
-include_lib("eunit/include/eunit.hrl").

w1() -> {worker, 'game1@localhost'}.
w2() -> {worker, 'game2@localhost'}.
wheel() -> {wheel, 'game3@localhost'}.
snap() -> {7, 'game3@localhost'}.

new_non_registra_test() ->
    CL = cl_recorder:new(),
    ?assertNot(cl_recorder:is_recording(CL)),
    ?assertNot(cl_recorder:is_complete(CL)).

%% L'iniziatore apre TUTTI i canali entranti e non e' completo finche'
%% non li ha chiusi uno per uno.
start_apre_tutti_i_canali_test() ->
    CL = cl_recorder:start(snap(), #{bets => []}, [w1(), w2()]),
    ?assert(cl_recorder:is_recording(CL)),
    ?assertNot(cl_recorder:is_complete(CL)),
    CL1 = cl_recorder:close(w1(), CL),
    ?assertNot(cl_recorder:is_complete(CL1)),
    CL2 = cl_recorder:close(w2(), CL1),
    ?assert(cl_recorder:is_complete(CL2)).

%% Il primo marker apre gli altri canali, non quello da cui e' arrivato.
primo_marker_apre_gli_altri_test() ->
    {CL, Kind} = cl_recorder:on_marker(snap(), wheel(), [wheel()], #{unacked => []},
                                       cl_recorder:new()),
    ?assertEqual(first_marker, Kind),
    ?assertEqual(snap(), cl_recorder:id(CL)),
    ?assertEqual(#{unacked => []}, cl_recorder:local(CL)),
    %% unico canale entrante ed e' quello del marker: taglio gia' chiuso
    ?assert(cl_recorder:is_complete(CL)).

%% Un marker successivo chiude SOLO il proprio canale.
marker_successivo_chiude_solo_il_suo_test() ->
    CL = cl_recorder:start(snap(), #{}, [w1(), w2()]),
    {CL1, Kind} = cl_recorder:on_marker(snap(), w1(), [w1(), w2()], #{}, CL),
    ?assertEqual(subsequent, Kind),
    ?assertNot(cl_recorder:is_complete(CL1)),
    {CL2, subsequent} = cl_recorder:on_marker(snap(), w2(), [w1(), w2()], #{}, CL1),
    ?assert(cl_recorder:is_complete(CL2)).

%% I messaggi si accodano solo sui canali aperti, e nell'ordine d'arrivo.
on_app_msg_solo_su_canali_aperti_test() ->
    CL = cl_recorder:start(snap(), #{}, [w1(), w2()]),
    CL1 = cl_recorder:on_app_msg(w1(), bet_a, CL),
    CL2 = cl_recorder:on_app_msg(w1(), bet_b, CL1),
    CL3 = cl_recorder:close(w1(), CL2),
    %% dopo la chiusura del canale il messaggio NON entra nel taglio
    CL4 = cl_recorder:on_app_msg(w1(), bet_c, CL3),
    ?assertEqual(#{w1() => [bet_a, bet_b]}, cl_recorder:channels(CL4)).

on_app_msg_ignorato_se_non_registra_test() ->
    CL = cl_recorder:on_app_msg(w1(), bet_a, cl_recorder:new()),
    ?assertEqual(#{}, cl_recorder:channels(CL)).

close_all_chiude_il_taglio_test() ->
    CL = cl_recorder:start(snap(), #{}, [w1(), w2()]),
    ?assert(cl_recorder:is_complete(cl_recorder:close_all(CL))).

%% Un taglio nuovo sostituisce uno rimasto aperto (collector morto).
nuovo_taglio_sostituisce_il_precedente_test() ->
    CL = cl_recorder:start({6, 'game3@localhost'}, #{vecchio => true}, [w1(), w2()]),
    {CL1, Kind} = cl_recorder:on_marker(snap(), wheel(), [wheel()], #{nuovo => true}, CL),
    ?assertEqual(first_marker, Kind),
    ?assertEqual(snap(), cl_recorder:id(CL1)),
    ?assertEqual(#{nuovo => true}, cl_recorder:local(CL1)).
