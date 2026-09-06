import requests
import random
import time
import sys
from concurrent.futures import ThreadPoolExecutor

# Indirizzo del server (modifica qui o passa da riga di comando, es: python test_scripts/stress_test.py http://10.2.1.15:8080)
DEFAULT_HOST = "http://10.2.1.15:8080"
HOST = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HOST
N_USERS = 50
SEGMENTS = ["1", "2", "5", "10", "Pachinko", "CoinFlip", "CashHunt", "CrazyTime"]
PASSWORD = "password123"

# Mappa username -> session_token
user_tokens = {}

def register_and_login(username):
    """Registra l'utente se necessario ed effettua il login per ottenere il token JWT/Sessione."""
    # 1. Registrazione (se l'utente già esiste, ignora l'errore)
    try:
        requests.post(
            f"{HOST}/api/auth/register",
            json={"username": username, "password": PASSWORD, "initialBalance": 1000.00},
            timeout=5
        )
    except Exception:
        pass

    # 2. Login per ottenere il session token
    try:
        res = requests.post(
            f"{HOST}/api/auth/login",
            json={"username": username, "password": PASSWORD},
            timeout=5
        )
        if res.status_code == 200:
            data = res.json()
            token = data.get("token")
            user_tokens[username] = token
            return True
        else:
            print(f" Errore login per {username}: {res.text}")
            return False
    except Exception as e:
        print(f" Errore connessione login per {username}: {e}")
        return False

def get_balance(username):
    token = user_tokens.get(username)
    if not token:
        return 0.0
    headers = {"Authorization": f"Bearer {token}"}
    try:
        res = requests.get(f"{HOST}/api/wallet/balance", headers=headers, timeout=5)
        if res.status_code == 200:
            data = res.json()
            return float(data.get("balance", 0.0))
        return 0.0
    except Exception:
        return 0.0

def place_bet(username, amount, segment):
    token = user_tokens.get(username)
    if not token:
        return False
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {"amount": amount, "segment": segment}
    try:
        res = requests.post(
            f"{HOST}/api/wallet/place-bet",
            headers=headers,
            json=payload,
            timeout=5
        )
        return res.status_code == 200
    except Exception:
        return False

def fetch_all_balances(users):
    balances = {}
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(get_balance, u): u for u in users}
        for future in futures:
            u = futures[future]
            balances[u] = future.result()
    return balances

if __name__ == "__main__":
    print(f"=== PREPARAZIONE STRESS TEST ===")
    print(f" Connessione verso: {HOST}")
    print(f" Utenti da simulare: {N_USERS}\n")

    users = [f"player_{i}" for i in range(1, N_USERS + 1)]

    print(f"Registrazione e Login di {N_USERS} utenti in corso...")
    with ThreadPoolExecutor(max_workers=25) as executor:
        results = list(executor.map(register_and_login, users))
    
    logged_in_count = sum(1 for r in results if r)
    if logged_in_count == 0:
        print(f"\nERRORE CRITICO: Nessun utente è riuscito a connettersi al server ({HOST}).")
        print("Verifica che il Java Gateway sia avviato e che l'indirizzo HOST sia corretto.")
        print(f"Esempio d'uso: python test_scripts/stress_test.py http://10.2.1.15:8080 oppure http://localhost:8080")
        sys.exit(1)
    
    print(f"{logged_in_count}/{N_USERS} utenti autenticati con successo!\n")
    round_numero = 1

    while True:
        print("="*75)
        print(f"PREPARAZIONE ROUND {round_numero}")
        print("="*75)
        
        # Saldo iniziale prima delle scommesse
        balances_initial = fetch_all_balances(users)
        bets_placed = {}
        
        # Generazione casuale delle scommesse per ogni utente (multipli di 0.10€ tra 0.10€ e 20.00€)
        for u in users:
            amt = round(random.randint(1, 200) * 0.10, 2)
            seg = random.choice(SEGMENTS)
            bets_placed[u] = {'amount': amt, 'segment': seg}

        print(" TUTTO PRONTO! Guarda la ruota sul browser.")
        input(" PREMI INVIO QUI APPENA PARTE IL TIMER DI SCOMMESSA (BETTING PHASE)... ")
        
        print("\n Invio scommesse in parallelo...")
        bet_results = {}
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {
                executor.submit(place_bet, u, bets_placed[u]['amount'], bets_placed[u]['segment']): u
                for u in users
            }
            for f in futures:
                u = futures[f]
                bet_results[u] = f.result()
                
        accepted = sum(1 for ok in bet_results.values() if ok)
        print(f" Scommesse inviate! Accettate dal server: {accepted}/{N_USERS}")

        print("\n" + "-"*75)
        input(" PREMI INVIO QUI APPENA LA RUOTA SI È FERMATA E IL ROUND È FINITO... ")

        print("\n=== RISULTATI DEL ROUND ===")
        balances_final = fetch_all_balances(users)
        vincitori = 0
        totale_vincite = 0.0

        print(f"{'UTENTE':<12} | {'SCOMMESSA':<20} | {'SALDO INIZ.':<12} | {'SALDO FIN.':<10} | {'RISULTATO'}")
        print("-" * 85)
        
        for u in users:
            start_bal = balances_initial[u]
            end_bal = balances_final[u]
            amt = bets_placed[u]['amount']
            seg = bets_placed[u]['segment']
            dettaglio = f"{amt}€ su {seg}"
            
            if not bet_results.get(u, False):
                print(f"{u:<12} | {dettaglio:<20} | {start_bal:<12.2f} | {end_bal:<10.2f} | SCOMMESSA RIFIUTATA")
                continue
            
            # Se ha perso, il saldo finale atteso è start_bal - amt
            expected_loss_bal = round(start_bal - amt, 2)
            payout = round(end_bal - expected_loss_bal, 2)
            
            if payout > 0.001:
                vincitori += 1
                totale_vincite += payout
                profitto_netto = round(payout - amt, 2)
                print(f"{u:<12} | {dettaglio:<20} | {start_bal:<12.2f} | {end_bal:<10.2f} | VINTO +{payout:.2f}€ (netto: {profitto_netto:+.2f}€)")
            else:
                print(f"{u:<12} | {dettaglio:<20} | {start_bal:<12.2f} | {end_bal:<10.2f} | PERSO (-{amt:.2f}€)")

        print("-" * 85)
        print(f"Vincitori totali : {vincitori}/{accepted}")
        print(f"Totale erogato dal banco : {totale_vincite:.2f}€")
        
        round_numero += 1
        print("\n...In attesa del prossimo round...")
        time.sleep(2)