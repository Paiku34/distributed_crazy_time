"""Test 6.4: bet in transito al momento del taglio Chandy-Lamport."""
import requests, time
from concurrent.futures import ThreadPoolExecutor
H="http://localhost:8080"
users=[f"player_{i}" for i in range(1,21)]
tok={u:requests.post(f"{H}/api/auth/login",json={"username":u,"password":"password123"},timeout=5).json()["token"] for u in users}
hd=lambda u:{"Authorization":f"Bearer {tok[u]}","Content-Type":"application/json"}
st=lambda:requests.get(f"{H}/api/game/state",headers=hd(users[0]),timeout=5).json()
# aspetta l'ultimo secondo della fase di betting
while True:
    s=st()
    if s["phase"]=="betting" and s["time_left"]==1: break
    time.sleep(0.1)
print(f"[*] ROUND #{s['round']}: invio 20 bet a time_left=1s (delay di forward: 600ms)")
t0=time.time()
def bet(u): return requests.post(f"{H}/api/wallet/place-bet",headers=hd(u),json={"amount":5.0,"segment":"2"},timeout=10).status_code==200
with ThreadPoolExecutor(max_workers=20) as ex: ok=sum(ex.map(bet,users))
print(f"[+] {ok}/20 bet accettate dal gateway in {time.time()-t0:.2f}s -> round {s['round']}")
