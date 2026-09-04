"""Test 6.3: crash del dealer (game3) durante la fase di betting."""
import requests, subprocess, time, json
H="http://localhost:8080"; PWD="password123"
users=[f"player_{i}" for i in range(1,11)]
tok={}
for u in users:
    tok[u]=requests.post(f"{H}/api/auth/login",json={"username":u,"password":PWD},timeout=5).json()["token"]
hd=lambda u:{"Authorization":f"Bearer {tok[u]}","Content-Type":"application/json"}
bal=lambda u:float(requests.get(f"{H}/api/wallet/balance",headers=hd(u),timeout=5).json()["balance"])
st=lambda:requests.get(f"{H}/api/game/state",headers=hd(users[0]),timeout=5).json()

while True:
    s=st()
    if s["phase"]=="betting" and s["time_left"]>=7: break
    time.sleep(0.3)
rnd=s["round"]
print(f"[*] Fase betting ROUND #{rnd} (time_left={s['time_left']}s)")
b0={u:bal(u) for u in users}
print(f"[*] Saldi prima delle puntate: {b0[users[0]]:.2f} ... (10 utenti)")
for u in users:
    ok=requests.post(f"{H}/api/wallet/place-bet",headers=hd(u),json={"amount":10.0,"segment":"5"},timeout=5).status_code==200
b1={u:bal(u) for u in users}
tot=sum(b0[u]-b1[u] for u in users)
print(f"[+] 10 puntate da 10.00 EUR su segmento '5' piazzate. Totale addebitato: {tot:.2f} EUR")
print(f"[*] Stato: {st()}")
print("[!] KILL -9 del dealer game3 PRIMA del gong...")
subprocess.run(["pkill","-9","-f","game3@localhost"])
t=time.time()
print(f"[!] game3 ucciso a t={time.strftime('%H:%M:%S')}")
for i in range(12):
    time.sleep(5)
    s=st()
    b=sum(bal(u) for u in users)
    print(f"  t+{int(time.time()-t):>3}s round={s['round']:<3} phase={s['phase']:<9} saldo_totale_10_utenti={b:.2f}")
b2={u:bal(u) for u in users}
rimb=sum(b2[u]-b1[u] for u in users)
print(f"\n[*] Saldo restituito dopo il crash: {rimb:.2f} EUR (puntato: {tot:.2f} EUR)")
for u in users[:5]:
    print(f"  {u:<10} prima={b0[u]:>8.2f}  dopo_bet={b1[u]:>8.2f}  fine={b2[u]:>8.2f}")
