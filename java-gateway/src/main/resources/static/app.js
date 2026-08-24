// =========================================================
//  DISTRIBUTED CRAZY TIME — app.js
//  Canvas wheel, 10s spin, animated minigames, auth flow
// =========================================================

// ===== CONSTANTS =====
const SEGMENTS = [
    "CrazyTime", "1", "2", "5", "1", "2",
    "Pachinko", "1", "5", "1", "2", "1",
    "CoinFlip", "1", "2", "1", "10", "2",
    "CashHunt", "1", "2", "1", "5", "1",
    "CoinFlip", "1", "5", "2", "10", "1",
    "Pachinko", "1", "2", "5", "1", "2",
    "CoinFlip", "1", "10", "1", "5", "1",
    "CashHunt", "1", "2", "5", "1", "2",
    "CoinFlip", "2", "1", "10", "2", "1"
];
const NUM_SEGMENTS = 54;

const SEGMENT_COLORS = {
    "1": "#2563eb",
    "2": "#eab308",
    "5": "#e844a0",
    "10": "#8b5cf6",
    "Pachinko": "#d946ef",
    "CoinFlip": "#ef4444",
    "CashHunt": "#10b981",
    "CrazyTime": "#f59e0b"
};

const SEGMENT_TEXT_SHORT = {
    "1": "1", "2": "2", "5": "5", "10": "10",
    "Pachinko": "PACH", "CoinFlip": "FLIP",
    "CashHunt": "HUNT", "CrazyTime": "CT"
};

// ===== STATE =====
let currentUser = null;
let currentBalance = 0;
let stompClient = null;
let shouldReconnect = true;

async function authFetch(url, options = {}) {
    const token = sessionStorage.getItem('token');
    options.headers = options.headers || {};
    if (token) {
        options.headers['Authorization'] = `Bearer ${token}`;
    }
    
    if (options.body && typeof options.body === 'object' && !(options.body instanceof FormData)) {
        options.body = JSON.stringify(options.body);
        options.headers['Content-Type'] = 'application/json';
    }

    try {
        const response = await fetch(url, options);
        if (response.status === 401) {
            handleSessionInvalidated();
            return { ok: false, status: 401, json: async () => ({ success: false, error: "Non autorizzato" }) };
        }
        return response;
    } catch (e) {
        throw e;
    }
}

function handleSessionInvalidated(msg) {
    shouldReconnect = false;
    if (stompClient) {
        stompClient.disconnect();
    }
    sessionStorage.removeItem('token');
    currentUser = null;
    
    let reason = "La sessione è scaduta o ti sei loggato da un altro dispositivo.";
    if (msg && msg.body) {
        try {
            const data = JSON.parse(msg.body);
            if (data.reason === 'new_login') {
                reason = "Ti sei connesso da un altro dispositivo.";
            }
        } catch(e){}
    }
    alert(reason);
    
    // Reset auth form to clean login state
    document.getElementById('username').value = '';
    document.getElementById('password').value = '';
    document.getElementById('initial-balance').value = '1000';
    authError.textContent = '';
    isLoginMode = true;
    tabLogin.classList.add('active');
    tabRegister.classList.remove('active');
    registerFields.style.display = 'none';
    authBtnText.textContent = 'Entra nel Gioco';
    
    authScreen.style.display = 'block';
    gameScreen.style.display = 'none';
}

let myBetsThisRound = {};
let betSlip = {};
let isBetSlipSubmitted = false;
let wheelAngle = 0;
let isSpinning = false;
let spinAnimationId = null;
let lastResults = [];

// ===== DOM =====
const authScreen = document.getElementById('auth-screen');
const gameScreen = document.getElementById('game-screen');
const tabLogin = document.getElementById('tab-login');
const tabRegister = document.getElementById('tab-register');
const registerFields = document.getElementById('register-fields');
const authForm = document.getElementById('auth-form');
const authBtn = document.getElementById('auth-btn');
const authBtnText = document.getElementById('auth-btn-text');
const authSpinner = document.getElementById('auth-spinner');
const authError = document.getElementById('auth-error');
// logoutBtn removed

// playerName removed
const playerBalance = document.getElementById('player-balance');
const phaseText = document.getElementById('phase-text-overlay');

const wheelCanvas = document.getElementById('wheel-canvas');
const wheelCenterText = document.getElementById('wheel-center-text');
const lastResultsContainer = document.getElementById('last-results');

const chipHitboxes = document.querySelectorAll('.chip-hitbox');
const betHitboxes = document.querySelectorAll('.bet-hitbox');
const betAllHitboxes = document.querySelectorAll('.bet-all-hitbox');
const totalBetDisplay = document.getElementById('total-bet-display');

let selectedChipAmount = 0.10;
let totalBetThisRound = 0;

const minigameOverlay = document.getElementById('minigame-overlay');
const minigameArea = document.getElementById('minigame-animation-area');

// ===== BACKGROUND PARTICLES =====
(function initParticles() {
    const container = document.getElementById('bg-particles');
    for (let i = 0; i < 20; i++) {
        const p = document.createElement('div');
        p.className = 'particle';
        p.style.left = Math.random() * 100 + '%';
        p.style.animationDuration = (8 + Math.random() * 12) + 's';
        p.style.animationDelay = (Math.random() * 10) + 's';
        p.style.width = p.style.height = (2 + Math.random() * 3) + 'px';
        container.appendChild(p);
    }
})();

// ===== CANVAS WHEEL =====
function drawWheel(rotation) {
    if (!wheelCanvas) return;
    const ctx = wheelCanvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const size = 500;
    wheelCanvas.width = size * dpr;
    wheelCanvas.height = size * dpr;
    ctx.scale(dpr, dpr);

    const cx = size / 2;
    const cy = size / 2;

    ctx.clearRect(0, 0, size, size);
    ctx.save();
    ctx.translate(cx, cy);
    ctx.rotate(rotation);

    if (!window.wheelImg) {
        window.wheelImg = new Image();
        window.wheelImg.src = 'img/wheel.png';
        window.wheelImg.onload = () => drawWheel(rotation);
    } else if (window.wheelImg.complete && window.wheelImg.naturalWidth > 0) {
        // Draw the image centered
        ctx.drawImage(window.wheelImg, -cx, -cy, size, size);
    }

    ctx.restore();
}

// Initial draw
drawWheel(0);

// ===== SPIN ANIMATION =====
function spinWheel(targetIndex, duration, onComplete) {
    if (isSpinning) return;
    isSpinning = true;
    clearDevSelection();

    const arcAngle = (2 * Math.PI) / NUM_SEGMENTS;

    // Assuming the image has index 0 (CrazyTime) perfectly centered at the top (12 o'clock)
    // To land on targetIndex, we just rotate backwards by targetIndex * arcAngle
    // (We also add a random offset within the segment so it doesn't land perfectly center every time)
    const randomOffset = (Math.random() * 0.8 - 0.4) * arcAngle;
    const targetAngle = -(targetIndex * arcAngle) + randomOffset;
    const fullRotations = 5 * 2 * Math.PI; // 5 full spins
    const totalAngle = fullRotations + targetAngle - (wheelAngle % (2 * Math.PI));

    // Ensure we always spin forward (positive direction)
    const finalAngle = wheelAngle + totalAngle + (totalAngle < 0 ? 2 * Math.PI : 0);

    const startAngle = wheelAngle;
    const startTime = performance.now();

    function easeOutQuint(t) {
        return 1 - Math.pow(1 - t, 5);
    }

    function animate(now) {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const eased = easeOutQuint(progress);

        wheelAngle = startAngle + (finalAngle - startAngle) * eased;
        drawWheel(wheelAngle);

        if (progress < 1) {
            spinAnimationId = requestAnimationFrame(animate);
        } else {
            isSpinning = false;
            wheelAngle = finalAngle;
            if (onComplete) onComplete();
        }
    }

    spinAnimationId = requestAnimationFrame(animate);
}

// ===== AUTH =====
let isLoginMode = true;

tabLogin.addEventListener('click', () => {
    isLoginMode = true;
    tabLogin.classList.add('active');
    tabRegister.classList.remove('active');
    registerFields.style.display = 'none';
    authBtnText.textContent = 'Entra nel Gioco';
    authError.textContent = '';
});

tabRegister.addEventListener('click', () => {
    isLoginMode = false;
    tabRegister.classList.add('active');
    tabLogin.classList.remove('active');
    registerFields.style.display = 'block';
    authBtnText.textContent = 'Crea Account';
    authError.textContent = '';
});

authForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const username = document.getElementById('username').value.trim();
    const password = document.getElementById('password').value;

    if (!username) { authError.textContent = 'Inserisci un username'; return; }
    if (password.length < 4) { authError.textContent = 'Password deve avere almeno 4 caratteri'; return; }

    authBtnText.style.display = 'none';
    authSpinner.style.display = 'block';
    authBtn.disabled = true;

    try {
        if (isLoginMode) {
            await doLogin(username, password);
        } else {
            const initialBalance = document.getElementById('initial-balance').value || '1000';
            await doRegister(username, password, initialBalance);
        }
    } finally {
        authBtnText.style.display = 'inline';
        authSpinner.style.display = 'none';
        authBtn.disabled = false;
    }
});

async function doLogin(username, password) {
    try {
        const res = await fetch(`/api/auth/login`, { 
            method: 'POST', 
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({username, password}) 
        });
        const data = await res.json();
        if (data.success) {
            sessionStorage.setItem('token', data.token);
            currentUser = data.username;
            currentBalance = data.balance;
            shouldReconnect = true;
            startGame();
        } else {
            authError.textContent = data.error || 'Login fallito';
        }
    } catch (e) {
        authError.textContent = 'Errore di connessione al server';
    }
}

async function doRegister(username, password, initialBalance) {
    try {
        const res = await fetch(`/api/auth/register`, { 
            method: 'POST', 
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({username, password, initialBalance}) 
        });
        const data = await res.json();
        if (data.success) {
            authError.textContent = '';
            await doLogin(username, password);
        } else {
            authError.textContent = data.error || 'Registrazione fallita';
        }
    } catch (e) {
        authError.textContent = 'Errore di connessione al server';
    }
}



// ===== GAME START =====
function startGame() {
    authScreen.style.display = 'none';
    gameScreen.style.display = 'flex';
    const sideUsername = document.getElementById('side-username');
    if(sideUsername) sideUsername.textContent = currentUser;
    // playerName removed
    updateBalanceDisplay(currentBalance);
    updateDevPanelVisibility();
    connectWebSocket();

    authFetch('/api/game/state')
        .then(res => res.json())
        .then(data => {
            if (data.success) handleGameState(data);
        })
        .catch(() => { });
}

function getBetSlipTotal() {
    return Object.values(betSlip).reduce((sum, val) => sum + (parseFloat(val) || 0), 0);
}

function getAvailableBalance() {
    return Math.max(0, currentBalance - getBetSlipTotal());
}

function updateChipAvailability() {
    const avail = getAvailableBalance();
    chipHitboxes.forEach(chip => {
        const amount = parseFloat(chip.dataset.amount);
        if (amount > avail + 0.001) {
            chip.style.opacity = '0.35';
            chip.style.filter = 'grayscale(80%)';
            chip.style.cursor = 'not-allowed';
        } else {
            chip.style.opacity = '1';
            chip.style.filter = 'none';
            chip.style.cursor = 'pointer';
        }
    });
}

function updateBalanceDisplay(amount) {
    currentBalance = parseFloat(amount);
    playerBalance.textContent = `$${currentBalance.toFixed(2)}`;
    const sideBalance = document.getElementById('side-balance');
    if(sideBalance) sideBalance.textContent = `$${currentBalance.toFixed(2)}`;
    updateChipAvailability();
}

async function submitBetSlip() {
    if (isBetSlipSubmitted) return;
    const entries = Object.entries(betSlip).filter(([_, amt]) => amt > 0);
    if (entries.length === 0) return;

    isBetSlipSubmitted = true;
    const payload = entries.map(([segment, amount]) => ({
        segment: segment,
        amount: parseFloat(amount.toFixed(2))
    }));

    try {
        const res = await authFetch('/api/wallet/place-bet', {
            method: 'POST',
            body: payload
        });
        const data = await res.json();
        if (data.success) {
            updateBalanceDisplay(data.new_balance);
            myBetsThisRound = { ...betSlip };
            console.log('[BET SLIP] Scommesse confermate con successo:', data);
        } else {
            showBetError(data.error || 'Errore durante la scommessa');
            if (currentPhase === 'betting') {
                isBetSlipSubmitted = false;
            }
        }
    } catch (e) {
        console.error('[BET SLIP] Errore connessione:', e);
        if (currentPhase === 'betting') {
            isBetSlipSubmitted = false;
        }
    }
}

// ===== WEBSOCKET =====
function connectWebSocket() {
    if (!shouldReconnect) return;
    const token = sessionStorage.getItem('token');
    const socket = new SockJS('/ws' + (token ? '?token=' + token : ''));
    stompClient = Stomp.over(socket);
    stompClient.debug = null;

    stompClient.connect({}, function () {
        stompClient.subscribe('/user/queue/session', handleSessionInvalidated);

        stompClient.subscribe('/topic/game-timer', function (msg) {
            const data = JSON.parse(msg.body);
            handleGameState(data);
        });

        stompClient.subscribe('/topic/game-results', function (msg) {
            const data = JSON.parse(msg.body);
            handleGameResult(data);
        });
    }, function () {
        // Reconnect after 3 seconds
        setTimeout(connectWebSocket, 3000);
    });
}

// ===== GAME STATE HANDLER =====
let currentPhase = '';
let pendingWinnerIndex = null;
let pendingWinner = null;
let isMinigamePlaying = false;

function handleGameState(data) {
    if (data.history && Array.isArray(data.history)) {
        // Only re-render if the history actually changed to prevent flickering every second
        if (JSON.stringify(historyItems) !== JSON.stringify(data.history)) {
            historyItems = data.history;
            renderLastResults();
        }
    }

    const phase = data.phase;
    if (phase === 'betting') {
        currentPhase = 'betting';
        phaseText.textContent = `PUNTATE APERTE: ${data.time_left}s`;
        phaseText.className = 'timer-phase betting';

        // Reset round on fresh betting phase
        if (data.time_left >= 9) {
            clearChips();
            betSlip = {};
            myBetsThisRound = {};
            isBetSlipSubmitted = false;
            wheelCenterText.innerHTML = 'CRAZY<br>TIME';
            document.body.classList.remove('minigame-active', 'coinflip-active', 'cashhunt-active', 'crazytime-active');
            minigameOverlay.classList.remove('visible');
            updateChipAvailability();
        }

        // Invio automatico della schedina a 1s dalla fine del countdown (mitiga jitter di rete)
        if (data.time_left <= 1 && !isBetSlipSubmitted && Object.keys(betSlip).length > 0) {
            submitBetSlip();
        }

    } else if (phase === 'spinning') {
        currentPhase = 'spinning';
        if (!isBetSlipSubmitted && Object.keys(betSlip).length > 0) {
            submitBetSlip();
        }
        clearDevSelection();
        phaseText.textContent = 'SCOMMESSE CHIUSE';
        phaseText.className = 'timer-phase spinning';
        // betButtons removed

        // If we received winner_index, spin to it
        if (data.winner_index !== undefined && !isSpinning) {
            pendingWinnerIndex = data.winner_index;
            pendingWinner = data.winner;
            wheelCenterText.innerHTML = '...';
            spinWheel(data.winner_index, 10000, () => {
                // After spin completes, show the winner on center
                wheelCenterText.innerHTML = data.winner || '?';
            });
        }

    } else if (phase === 'minigame') {
        currentPhase = 'minigame';
        phaseText.textContent = `BONUS: ${data.minigame}`;
        phaseText.className = 'timer-phase minigame';
        
        // Start async minigame immediately if it has details (e.g. CrazyTime)
        if (data.details && !window.activeMinigame) {
            window.activeMinigame = data.minigame;
            showMinigameAnimation(data.minigame, 0, data.details, () => {
                window.activeMinigame = null;
            });
        }
    } else if (phase === 'cooldown') {
    }
}

// ===== GAME RESULT HANDLER =====
function handleGameResult(data) {
    // Il dealer e' caduto a meta' round: le puntate non rigiocabili sono gia'
    // state rimborsate dal gateway, qui si avvisa e si ricarica il saldo.
    if (data.type === 'round_cancelled') {
        showRoundCancelled(data.round);
        myBetsThisRound = {};
        fetchBalance();
        return;
    }

    wheelCenterText.innerHTML = data.winner || '?';

    // Calculate if player won
    const myBetAmount = myBetsThisRound[data.winner];
    const isWin = myBetAmount !== undefined;
    let winAmount = 0;
    
    // data.multiplier is a -1 sentinel for async minigames (CrazyTime/CashHunt), telling the
    // backend to use the payouts array instead — never display it directly.
    let myMultiplier = data.multiplier >= 0 ? data.multiplier : 0;
    if (isWin) {
        if (data.payouts && Array.isArray(data.payouts)) {
            const myPayout = data.payouts.find(p => p.username === currentUser);
            if (myPayout) {
                winAmount = myPayout.payout;
                myMultiplier = myBetAmount > 0 ? Math.round((winAmount - myBetAmount) / myBetAmount) : 0;
            } else if (data.multiplier >= 0) {
                winAmount = myBetAmount + (myBetAmount * data.multiplier);
            }
        } else if (data.multiplier >= 0) {
            winAmount = myBetAmount + (myBetAmount * data.multiplier);
        }
    }

    // Async minigames (CrazyTime/CashHunt) report result_type "async_minigame", not "minigame".
    const isMinigame = data.result_type === 'minigame' || data.result_type === 'async_minigame';
    const details = data.details || {};

    if (isMinigame && data.winner) {
        if (window.activeMinigame === data.winner) {
            // Minigame already running asynchronously, just show the final result
            showResult(isWin, data.winner, myMultiplier, winAmount);
            if (isWin) fetchBalance();
            window.activeMinigame = null;
        } else {
            // Show minigame animation first, then result
            showMinigameAnimation(data.winner, myMultiplier, details, () => {
                showResult(isWin, data.winner, myMultiplier, winAmount);
                if (isWin) fetchBalance();
            });
        }
    } else {
        // Direct multiplier — show result immediately after spin
        const delay = isSpinning ? 500 : 100;
        setTimeout(() => {
            document.body.classList.remove('minigame-active');
            showResult(isWin, data.winner, myMultiplier, winAmount);
            if (isWin) fetchBalance();
        }, delay);
    }
}

function showResult(isWin, winner, multiplier, winAmount) {
    document.body.classList.remove('minigame-active', 'coinflip-active', 'cashhunt-active');

    // Safety net: after the 1s CSS transition, forcefully hide the overlay so it can't block the wheel
    setTimeout(() => {
        if (!isMinigamePlaying) {
            minigameOverlay.style.display = 'none';
        }
    }, 1000);

    // Create the epic multiplier animation element
    const multAnim = document.createElement('div');
    multAnim.className = 'epic-multiplier-anim';
    multAnim.textContent = `${multiplier}X`;
    document.body.appendChild(multAnim);

    // Clean up multiplier element after animation ends (3s)
    setTimeout(() => {
        multAnim.remove();
    }, 3000);

    // If the user actually won money, show the win amount trailing the multiplier
    if (isWin && winAmount > 0) {
        setTimeout(() => {
            const winAnim = document.createElement('div');
            winAnim.className = 'epic-win-anim';
            winAnim.textContent = `+$${winAmount.toFixed(2)}`;
            document.body.appendChild(winAnim);

            setTimeout(() => {
                winAnim.remove();
            }, 3000);
        }, 2800); // Play after the multiplier animation finishes
    }
}

const MAX_HISTORY = 21;
let historyItems = [];

// `addLastResult` removed since history is synced from backend

function renderLastResults() {
    lastResultsContainer.innerHTML = historyItems.map(r => {
        const cssName = r.winner;
        let display = r.multiplier > 1 ? `${r.multiplier}x` : (r.winner === 'CrazyTime' ? 'CT' : (r.winner === 'Pachinko' ? 'PACH' : (r.winner === 'CoinFlip' ? 'FLIP' : (r.winner === 'CashHunt' ? 'HUNT' : r.winner))));
        return `<div class="history-item hist-${cssName}">${display}</div>`;
    }).join('');
}

// ===== MINIGAME ANIMATIONS =====

function getMinigameEmoji(name) {
    const emojis = { Pachinko: '🔴', CoinFlip: '🪙', CashHunt: '🎯', CrazyTime: '🎡' };
    return emojis[name] || '🎰';
}

function showMinigameAnimation(name, multiplier, details, onComplete) {
    isMinigamePlaying = true;

    // Bulletproof against cached index.html
    minigameOverlay.style.display = '';

    if (name === 'CoinFlip') {
        minigameOverlay.className = 'coinflip-slide-overlay';
        // Forza il reflow del browser per far funzionare l'animazione CSS
        void minigameOverlay.offsetWidth;
        document.body.classList.add('coinflip-active');
    } else if (name === 'CashHunt') {
        minigameOverlay.className = 'cashhunt-slide-overlay';
        void minigameOverlay.offsetWidth;
        document.body.classList.add('cashhunt-active');
    } else if (name === 'Pachinko') {
        minigameOverlay.className = 'pachinko-slide-overlay';
        void minigameOverlay.offsetWidth;
        document.body.classList.add('pachinko-active');
    } else if (name === 'CrazyTime') {
        minigameOverlay.className = 'ct-overlay';
        void minigameOverlay.offsetWidth;
        document.body.classList.add('crazytime-active');
    } else {
        // Fallback or other minigames (per user request: "lascia perdere gli altri minigiochi")
        minigameOverlay.className = 'fullscreen-overlay';
        minigameOverlay.style.display = 'flex';
        document.body.classList.add('minigame-active');
    }

    minigameArea.className = 'minigame-area-full';

    const wrappedComplete = () => {
        isMinigamePlaying = false;
        document.body.classList.remove('pachinko-active', 'cashhunt-active', 'crazytime-active', 'minigame-active');
        minigameOverlay.classList.remove('visible');
        onComplete();
    };

    switch (name) {
        case 'Pachinko':
            animatePachinko(multiplier, details, wrappedComplete);
            break;
        case 'CoinFlip':
            animateCoinFlip(multiplier, details, wrappedComplete);
            break;
        case 'CashHunt':
            animateCashHunt(multiplier, details, wrappedComplete);
            break;
        case 'CrazyTime':
            animateCrazyTime(multiplier, details, wrappedComplete);
            break;
        default:
            setTimeout(wrappedComplete, 2000);
            console.log("No custom animation for", name);
    }
}

function showMinigameResultMultiplier(multiplier) {
    // Disabilitato come richiesto: il moltiplicatore viene già mostrato nella ruota principale a fine round
    /*
    const res = document.createElement('div');
    res.style.cssText = 'position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:5rem;font-weight:900;color:#fbbf24;text-shadow:0 0 30px rgba(0,0,0,0.9);z-index:100;animation:popIn 0.5s ease;background:rgba(0,0,0,0.6);padding:20px 40px;border-radius:20px;border:3px solid #fbbf24;';
    res.textContent = `x${multiplier}`;
    minigameArea.appendChild(res);
    */
}



// --- COIN FLIP ---
function animateCoinFlip(multiplier, details, onComplete) {
    const sideA = details.side_a || 5;
    const sideB = details.side_b || 10;
    const winnerSide = details.winner_side || 'heads';

    let html = `
        <div class="coinflip-container" style="position: absolute; inset: 0; width: 100%; height: 100%; background-image: url('img/coinflip.png'); background-size: 100% 100%; background-position: center; overflow: hidden;">
            
            <!-- Nessun titolo centrale così non copre l'immagine -->

            <!-- Moltiplicatori posizionati verticalmente (Sopra/Sotto) -->
            <div id="cf-side-a" style="position: absolute; top: 28%; left: 42%; transform: translate(-50%, -50%); display: flex; flex-direction: column; align-items: center; justify-content: center; z-index: 10; text-align: center;">
                <div class="side-mult" id="cf-disp-mult-a" style="font-size: 1.5rem; font-weight: 800; color: #fff; text-shadow: 0 0 10px #000, 0 0 20px #ff4b4b;">x?</div>
            </div>
            
            <div id="cf-side-b" style="position: absolute; top: 45%; left: 42%; transform: translate(-50%, -50%); display: flex; flex-direction: column; align-items: center; justify-content: center; z-index: 10; text-align: center;">
                <div class="side-mult" id="cf-disp-mult-b" style="font-size: 1.5rem; font-weight: 800; color: #fff; text-shadow: 0 0 10px #000, 0 0 20px #4b4bff;">x?</div>
            </div>

            <!-- Moneta vera molto più piccola, posizionata nel box nero in basso -->
            <div style="position: absolute; bottom: 32%; left: 55%; transform: translateX(-50%); z-index: 10;">
                <div class="coinflip-coin" id="cf-coin" style="opacity: 0; transform: scale(0.5); transition: opacity 0.5s ease, transform 0.5s ease; box-shadow: 0 5px 20px rgba(0,0,0,0.9); border-radius: 50%; width: 90px; height: 90px;">
                    <div class="coin-side coin-heads" style="background: radial-gradient(circle at 30% 30%, #ff4b4b, #990000);">
                        <span id="cf-coin-mult-a" style="font-size: 1.5rem;">x?</span>
                        <span class="coin-label" style="font-size: 0.6rem;">ROSSO</span>
                    </div>
                    <div class="coin-side coin-tails" style="background: radial-gradient(circle at 30% 30%, #4b4bff, #000099);">
                        <span id="cf-coin-mult-b" style="font-size: 1.5rem;">x?</span>
                        <span class="coin-label" style="font-size: 0.6rem;">BLU</span>
                    </div>
                </div>
            </div>
        </div>
    `;

    minigameArea.innerHTML = html;

    const coin = document.getElementById('cf-coin');
    const coinMultA = document.getElementById('cf-coin-mult-a');
    const coinMultB = document.getElementById('cf-coin-mult-b');
    const dispMultA = document.getElementById('cf-disp-mult-a');
    const dispMultB = document.getElementById('cf-disp-mult-b');

    // Shuffle multipliers animation
    const possibleMults = [2, 3, 5, 7, 10, 15, 20, 25, 50, 100];
    let shuffleInterval = setInterval(() => {
        let r1 = possibleMults[Math.floor(Math.random() * possibleMults.length)];
        let r2 = possibleMults[Math.floor(Math.random() * possibleMults.length)];
        dispMultA.textContent = `x${r1}`;
        dispMultB.textContent = `x${r2}`;
    }, 250);

    // Stop shuffle and settle
    setTimeout(() => {
        clearInterval(shuffleInterval);
        coinMultA.textContent = `x${sideA}`;
        dispMultA.textContent = `x${sideA}`;
        coinMultB.textContent = `x${sideB}`;
        dispMultB.textContent = `x${sideB}`;

        // Blink to show they settled
        dispMultA.style.transform = "scale(1.2)";
        dispMultB.style.transform = "scale(1.2)";

        // Make the coin appear
        coin.style.opacity = '1';
        coin.style.transform = 'scale(1)';

        setTimeout(() => {
            dispMultA.style.transform = "scale(1)";
            dispMultB.style.transform = "scale(1)";
        }, 400);

        // Start flipping after a brief pause
        setTimeout(() => {
            coin.style.transform = ''; // Clear inline transform so CSS classes work
            coin.classList.add(winnerSide === 'heads' ? 'flipping-heads' : 'flipping-tails');

            setTimeout(() => {
                // Highlight winner side
                const winnerId = winnerSide === 'heads' ? 'cf-side-a' : 'cf-side-b';
                document.getElementById(winnerId).classList.add('winner-side');

                showMinigameResultMultiplier(multiplier);
                setTimeout(onComplete, 4000);
            }, 3500);
        }, 2000);
    }, 4500);
}

// --- CASH HUNT ---
function animateCashHunt(multiplier, details, onComplete) {
    const grid = details.grid || [];
    const initialGrid = details.initial_grid || grid;
    const cols = details.cols || 9;
    const rows = details.rows || 12;
    const defaultCell = details.default_cell || 0;
    const totalCells = cols * rows;

    const emojis = ['🎯', '🐰', '⭐', '🎪', '🎲', '🍀', '💎', '🦊', '🎵', '🎈', '🔔', '🌟', '🎃', '🍎', '🎁', '🦄', '🐻', '🎮', '🏆', '🎺', '🌈', '🍕', '🎭', '🦋'];
    const scrollMults = [2, 3, 5, 7, 10, 15, 20, 25, 50, 75, 100, 200];
    let html = `
        <style>
            .ch-container { 
                position: absolute; inset: 0; width: 100%; height: 100%;
                background-image: url('img/cashhunt.png'); background-size: 100% 100%; background-position: center; overflow: hidden;
            }
            .ch-grid-positioner {
                position: absolute; 
                top: 6%; /* Regola questo valore per alzare o abbassare il box */
                left: 35%; /* Regola questo valore per spostare il box a destra o sinistra */
                width: 45%; /* Regola la larghezza del box */
                height: 90%; /* Regola l'altezza del box */
                background: transparent;
                border: none;
                box-shadow: none;
                border-radius: 12px;
                padding: 10px;
                box-sizing: border-box;
                display: flex;
                flex-direction: column;
            }
            .ch-title {
                text-align: center; font-size: 2rem; font-weight: 900;
                background: linear-gradient(90deg, #10b981, #34d399, #10b981);
                -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                background-clip: text;
                letter-spacing: 3px; text-transform: uppercase;
                margin-bottom: 6px;
                animation: chTitlePulse 1.5s ease-in-out infinite alternate;
                position: absolute; top: 10px; left: 50%; transform: translateX(-50%);
            }
            @keyframes chTitlePulse { 0% { opacity: 0.8; filter: brightness(1); } 100% { opacity: 1; filter: brightness(1.3); } }
            .ch-timer-bar { 
                display: flex; align-items: center; justify-content: center; gap: 10px;
                margin-bottom: 10px; font-size: 1rem; color: #10b981; font-weight: 800;
                opacity: 0; transition: opacity 0.3s; height: 10%;
            }
            .ch-timer-bar.visible { opacity: 1; }
            .ch-timer-fill { width: 180px; height: 7px; background: rgba(255,255,255,0.1); border-radius: 4px; overflow: hidden; }
            .ch-timer-fill-inner { height: 100%; background: linear-gradient(90deg, #10b981, #059669); width: 100%; transition: width 1s linear; border-radius: 4px; }
            
            .ch-grid-wrapper { flex: 1; position: relative; border-radius: 6px; border: 2px solid rgba(16, 185, 129, 0.3); background: rgba(6, 78, 59, 0.8); padding: 5px; overflow: hidden; }
            
            .ch-grid {
                display: grid;
                grid-template-columns: repeat(${cols}, 1fr);
                grid-template-rows: repeat(${rows}, 1fr);
                gap: 2px; width: 100%; height: 100%;
            }
            .ch-cell {
                aspect-ratio: 1.4; display: flex; align-items: center; justify-content: center;
                font-weight: 900; font-size: 0.6rem; color: white; background: #1a1a2e;
                border-radius: 2px; cursor: default; transition: transform 0.15s, background 0.3s, box-shadow 0.3s;
                position: relative; overflow: hidden; user-select: none;
            }
            .ch-cell .ch-mult { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; font-weight: 900; font-size: 0.6rem; color: white; transition: opacity 0.4s; }
            .ch-cell .ch-emoji { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; font-size: 1rem; opacity: 0; transition: opacity 0.4s, transform 0.5s; }
            .ch-cell.covered .ch-mult { opacity: 0; }
            .ch-cell.covered .ch-emoji { opacity: 1; }
            .ch-cell.pickable { cursor: crosshair; }
            .ch-cell.pickable:hover { transform: scale(1.25); box-shadow: 0 0 12px rgba(251, 191, 36, 0.6); z-index: 10; }
            .ch-cell.picked { box-shadow: 0 0 15px #fbbf24 !important; border: 2px solid #fbbf24; transform: scale(1.25); z-index: 10; }
            .ch-cell.revealed .ch-emoji { opacity: 0; transform: scale(0.3) rotateZ(360deg); }
            .ch-cell.revealed .ch-mult { opacity: 1; }
            .ch-cell.winner-cell { background: linear-gradient(135deg, #fbbf24, #f59e0b) !important; box-shadow: 0 0 25px rgba(251, 191, 36, 0.8) !important; transform: scale(1.4) !important; z-index: 20; }
            .ch-cell.winner-cell .ch-mult { color: #1a1a2e !important; font-size: 0.8rem; }
            
            /* CSS HW Accelerated Scrolling Layer */
            .ch-scroll-layer { position: absolute; inset: 0; background: #0a0a1a; z-index: 5; overflow: hidden; display: flex; flex-direction: column; gap: 1px; padding: 2px; transition: opacity 0.5s; }
            .ch-scroll-row { flex: 1; display: flex; gap: 1px; overflow: hidden; white-space: nowrap; }
            .ch-scroll-item { flex: 0 0 calc((100% - 8px) / 9); display: flex; align-items: center; justify-content: center; background: #1a1a2e; color: white; font-weight: 900; font-size: 0.6rem; border-radius: 2px; }
            
            @keyframes scrollRightAnim { 0% { transform: translateX(-50%); } 100% { transform: translateX(0%); } }
            @keyframes scrollLeftAnim { 0% { transform: translateX(0%); } 100% { transform: translateX(-50%); } }
            
            .ch-msg { text-align: center; font-size: 1.1rem; font-weight: 800; color: #fbbf24; margin-top: 6px; min-height: 1.4em; }
            @keyframes chShuffleSlow { 0%,100% { transform: translate(0,0); } 50% { transform: translate(2px, -2px); } }
            @keyframes chShuffleFast { 0% { transform: translate(0,0); } 25% { transform: translate(4px,-3px); } 50% { transform: translate(-3px,3px); } 75% { transform: translate(3px,2px); } 100% { transform: translate(0,0); } }
            .ch-cell.shuffle-slow { animation: chShuffleSlow 0.3s ease-in-out infinite; }
            .ch-cell.shuffle-fast { animation: chShuffleFast 0.1s linear infinite; }
        </style>
        <div class="ch-container">
            <div class="ch-title">CASH HUNT</div>
            
            <!-- Box verde contenente la griglia da posizionare perfettamente sopra l'immagine -->
            <div class="ch-grid-positioner" id="cashhunt-target-box">
                <div class="ch-timer-bar" id="ch-timer">
                    <span style="color: white; font-size: 0.8rem;">SCEGLI IL BERSAGLIO!</span>
                    <div class="ch-timer-fill"><div class="ch-timer-fill-inner" id="ch-timer-fill"></div></div>
                    <span id="ch-timer-sec" style="color: white; font-size: 0.8rem;">10</span>
                </div>
                
                <div class="ch-grid-wrapper">
                    <div class="ch-scroll-layer" id="ch-scroll-layer"></div>
                    <div class="ch-grid" id="ch-grid">
                    </div>
                </div>
                <div class="ch-msg" id="ch-msg" style="color: white; font-size: 0.8rem; margin-top: 5px;">Moltiplicatori in arrivo...</div>
            </div>
        </div>
    `;

    minigameArea.innerHTML = html;

    const gridEl = document.getElementById('ch-grid');
    const scrollLayerEl = document.getElementById('ch-scroll-layer');
    const msgEl = document.getElementById('ch-msg');

    function colorMult(el, val) {
        if (val >= 100) el.style.color = '#fbbf24';
        else if (val >= 50) el.style.color = '#f59e0b';
        else if (val >= 15) el.style.color = '#a78bfa';
        else el.style.color = 'white';
    }

    // Populate actual grid (hidden behind scroll layer initially)
    for (let i = 0; i < totalCells; i++) {
        const cell = document.createElement('div');
        cell.className = 'ch-cell';
        cell.dataset.idx = i;
        cell.id = `ch-c-${i}`;
        const val = initialGrid[i] || 5;
        cell.innerHTML = `<div class="ch-mult" id="ch-m-${i}">x${val}</div><div class="ch-emoji" id="ch-e-${i}"></div>`;
        gridEl.appendChild(cell);
        colorMult(cell.querySelector('.ch-mult'), val);
    }

    // Populate CSS scrolling layer
    let scrollHtml = '';
    // Make strip twice as wide to allow scrolling without tearing
    const stripLength = cols * 3;
    for (let r = 0; r < rows; r++) {
        const isRight = r % 2 === 0;
        const animName = isRight ? 'scrollRightAnim' : 'scrollLeftAnim';
        scrollHtml += `<div class="ch-scroll-row" style="width: 200%; animation: ${animName} 8s cubic-bezier(0.1, 0.7, 0.1, 1) forwards;">`;
        for (let c = 0; c < stripLength; c++) {
            const mult = scrollMults[Math.floor(Math.random() * scrollMults.length)];
            let colColor = 'white';
            if (mult >= 100) colColor = '#fbbf24'; else if (mult >= 50) colColor = '#f59e0b'; else if (mult >= 15) colColor = '#a78bfa';
            scrollHtml += `<div class="ch-scroll-item" style="color: ${colColor};">x${mult}</div>`;
        }
        scrollHtml += `</div>`;
    }
    scrollLayerEl.innerHTML = scrollHtml;

    // === PHASE 1 & 2: CSS Scroll for 8s, then fade out layer ===
    setTimeout(() => {
        msgEl.textContent = 'Moltiplicatori assestati!';
        scrollLayerEl.style.opacity = '0';
        setTimeout(() => {
            scrollLayerEl.style.display = 'none';
            startPhase3();
        }, 500);
    }, 8000);

    // === PHASE 3: Cover with emojis ===
    function startPhase3() {
        setTimeout(() => {
            msgEl.textContent = 'Copertura in corso...';
            for (let r = 0; r < rows; r++) {
                setTimeout(() => {
                    for (let c = 0; c < cols; c++) {
                        const idx = r * cols + c;
                        const emojiEl = document.getElementById(`ch-e-${idx}`);
                        const cell = document.getElementById(`ch-c-${idx}`);
                        if (emojiEl) emojiEl.textContent = emojis[Math.floor(Math.random() * emojis.length)];
                        if (cell) cell.classList.add('covered');
                    }
                }, r * 60);
            }
            setTimeout(startPhase4, rows * 60 + 400);
        }, 1500);
    }

    // === PHASE 4: Shuffle with accelerate then decelerate (6s) ===
    function startPhase4() {
        msgEl.textContent = '\u{1F500} Shuffle!';
        const allCells = gridEl.querySelectorAll('.ch-cell');
        const shuffleDuration = 6000;
        const shuffleStartTime = performance.now();

        // Start slow
        allCells.forEach(c => c.classList.add('shuffle-slow'));
        setTimeout(() => {
            allCells.forEach(c => { c.classList.remove('shuffle-slow'); c.classList.add('shuffle-fast'); });
        }, 800);
        setTimeout(() => {
            allCells.forEach(c => { c.classList.remove('shuffle-fast'); c.classList.add('shuffle-slow'); });
        }, shuffleDuration - 1000);

        let swapTimeout;
        function doSwap() {
            const elapsed = performance.now() - shuffleStartTime;
            if (elapsed >= shuffleDuration) {
                allCells.forEach(c => { c.classList.remove('shuffle-slow', 'shuffle-fast'); });

                // CRITICAL: Removed local scramble to keep frontend grid synchronized with the server's grid
                // (Backend already provides the final shuffled grid)

                startPhase5();
                return;
            }
            const progress = elapsed / shuffleDuration;
            let swapDelay;
            if (progress < 0.2) swapDelay = 250;
            else if (progress < 0.7) swapDelay = 60;
            else swapDelay = 200 + (progress - 0.7) * 800;

            for (let i = 0; i < totalCells; i++) {
                const emojiEl = document.getElementById(`ch-e-${i}`);
                if (emojiEl) emojiEl.textContent = emojis[Math.floor(Math.random() * emojis.length)];
            }
            swapTimeout = setTimeout(doSwap, swapDelay);
        }
        doSwap();
    }

    // === PHASE 5: Player picks (10s timer) ===
    function startPhase5() {
        msgEl.textContent = '\u{1F3AF} SCEGLI UNA CELLA!';
        const timerBar = document.getElementById('ch-timer');
        timerBar.classList.add('visible');
        const timerFill = document.getElementById('ch-timer-fill');
        const timerSec = document.getElementById('ch-timer-sec');

        const pickDuration = 10;
        let pickedIndex = -1;
        let timeLeft = pickDuration;

        const allCells = gridEl.querySelectorAll('.ch-cell');
        allCells.forEach((cell) => {
            cell.classList.add('pickable');
            cell.addEventListener('click', function handler() {
                const cellIdx = parseInt(this.dataset.idx);
                if (pickedIndex >= 0) {
                    document.getElementById(`ch-c-${pickedIndex}`).classList.remove('picked');
                }
                pickedIndex = cellIdx;
                this.classList.add('picked');

                // Invia la scelta al server IMMEDIATAMENTE per evitare problemi di latenza
                if (currentUser) {
                    authFetch(`/api/game/choice`, {
                        method: 'POST',
                        body: {minigame: 'CashHunt', choice: pickedIndex.toString()}
                    }).catch(e => console.error("Errore invio scelta", e));
                }
            });
        });

        let countdownIv = setInterval(() => {
            timeLeft--;
            timerSec.textContent = timeLeft;
            timerFill.style.width = `${(timeLeft / pickDuration) * 100}%`;
            if (timeLeft <= 0) clearInterval(countdownIv);
        }, 1000);

        setTimeout(() => {
            clearInterval(countdownIv);
            timerBar.classList.remove('visible');

            allCells.forEach(cell => {
                cell.classList.remove('pickable');
                cell.style.cursor = 'default';
            });

            // Send default choice to backend solo se non ha mai cliccato
            if (pickedIndex < 0) {
                pickedIndex = defaultCell;
                if (currentUser) {
                    authFetch(`/api/game/choice`, {
                        method: 'POST',
                        body: {minigame: 'CashHunt', choice: pickedIndex.toString()}
                    }).catch(e => console.error("Errore invio scelta", e));
                }
            }

            startPhase6(pickedIndex);
        }, pickDuration * 1000);
    }

    // === PHASE 6: Reveal all cells ===
    function startPhase6(pickedIndex) {
        msgEl.textContent = '\u{1F389} Rivelazione!';
        
        // Calcola il vero moltiplicatore vinto dall'utente in base alla griglia
        const realMultiplier = grid[pickedIndex] || 5;

        // Update DOM with new scrambled grid
        for (let i = 0; i < totalCells; i++) {
            const el = document.getElementById(`ch-m-${i}`);
            const val = grid[i] || 5;
            if (el) { el.textContent = `x${val}`; colorMult(el, val); }
        }

        for (let r = 0; r < rows; r++) {
            setTimeout(() => {
                for (let c = 0; c < cols; c++) {
                    const idx = r * cols + c;
                    const cell = document.getElementById(`ch-c-${idx}`);
                    if (cell) cell.classList.add('revealed');
                }
            }, r * 50);
        }

        setTimeout(() => {
            const winnerCell = document.getElementById(`ch-c-${pickedIndex}`);
            if (winnerCell) winnerCell.classList.add('winner-cell');

            msgEl.textContent = `\u{1F3C6} Hai vinto: x${realMultiplier}!`;
            showMinigameResultMultiplier(realMultiplier);
            setTimeout(onComplete, 4000);
        }, rows * 50 + 800);
    }
}




// --- PACHINKO ---
function animatePachinko(multiplier, details, onComplete) {
    const drops = details.drops || [];
    if (drops.length === 0) {
        onComplete();
        return;
    }

    let html = `
        <style>
            .pk-container { 
                position: absolute; inset: 0; width: 100%; height: 100%;
                background-image: url('img/pachinko.png'); background-size: 100% 100%; background-position: center; overflow: hidden;
            }
            /* Box verde per il tabellone dei perni (puntini) e la fisica della pallina */
            .pk-board-positioner {
                position: absolute; 
                top: 15%; /* Modifica per alzare/abbassare il box puntini */
                left: 27%; /* Modifica per muovere il box puntini a destra/sinistra */
                width: 45%; /* Modifica per allargare/restringere il box puntini */
                height: 60%; /* Modifica l'altezza del box puntini */
                background: transparent;
                border: transparent;
                box-sizing: border-box;
                display: flex; flex-direction: column;
            }
            /* Box verde per i moltiplicatori finali in basso */
            .pk-slots-positioner {
                position: absolute; 
                top: 73%; /* Modifica per alzare/abbassare i moltiplicatori */
                left: 27%; /* Allinealo alla larghezza della board */
                width: 45%; 
                height: 12%; 
                background: transparent; 
                border: transparent;
                box-sizing: border-box;
                display: flex; gap: 4px; padding: 5px;
            }
            .pachinko-drop-zones { display: flex; width: 100%; height: 15px; margin-bottom: 5px; }
            .pk-dz { flex: 1; height: 100%; background: rgba(255,255,255,0.1); margin: 0 1px; transition: background 0.1s; border-radius: 5px; }
            
            .pachinko-slot { 
                flex: 1; display: flex; align-items: center; justify-content: center; 
                background: rgba(30, 30, 56, 0.9); border: 2px solid #8b5cf6; border-radius: 4px;
                font-weight: 900; font-size: 1rem; color: white; transition: transform 0.3s, background 0.3s; 
            }
            .pachinko-slot.winner-side {
                background: linear-gradient(135deg, #fbbf24, #f59e0b) !important;
                color: #1a1a2e; border-color: #fbbf24;
                box-shadow: 0 0 15px rgba(251, 191, 36, 0.5);
                transform: scale(1.15);
            }
        </style>
        <div class="pk-container">
            <!-- Box per perni e dropzone -->
            <div class="pk-board-positioner">
                <div class="pachinko-drop-zones" id="pk-drop-zones">
                    ${Array.from({ length: 16 }).map((_, i) => `<div class="pk-dz" id="pk-dz-${i}"></div>`).join('')}
                </div>
                <div style="flex: 1; position: relative; width: 100%;">
                    <canvas id="pk-canvas" style="width: 100%; height: 100%; display: block;"></canvas>
                </div>
            </div>
            
            <!-- Box per i moltiplicatori -->
            <div class="pk-slots-positioner" id="pk-slots">
                ${Array.from({ length: 8 }).map((_, i) => `<div class="pachinko-slot" id="pk-slot-${i}">x?</div>`).join('')}
            </div>
        </div>
    `;

    minigameArea.innerHTML = html;

    const canvas = document.getElementById('pk-canvas');
    canvas.width = canvas.offsetWidth * (window.devicePixelRatio || 1);
    canvas.height = canvas.offsetHeight * (window.devicePixelRatio || 1);
    const ctx = canvas.getContext('2d');
    ctx.scale(window.devicePixelRatio || 1, window.devicePixelRatio || 1);

    const boardW = canvas.offsetWidth;
    const boardH = canvas.offsetHeight;
    const pegRows = 15;
    const cols = 16;
    const pegRadius = 4;
    const ballRadius = 10;

    const pegs = [];
    for (let r = 0; r < pegRows; r++) {
        const numPegs = (r % 2 === 0) ? cols + 1 : cols;
        const rowY = (r + 1) * (boardH / (pegRows + 1.5));
        const spacingX = boardW / cols;
        const startX = (r % 2 === 0) ? 0 : spacingX / 2;
        for (let c = 0; c < numPegs; c++) {
            pegs.push({ x: startX + c * spacingX, y: rowY });
        }
    }

    function drawBoard() {
        ctx.clearRect(0, 0, boardW, boardH);
        ctx.fillStyle = 'rgba(255, 255, 255, 0)'; // Trasparenza per i perni virtuali
        ctx.shadowColor = 'rgba(255, 255, 255, 0)';
        ctx.shadowBlur = 5;
        pegs.forEach(p => {
            ctx.beginPath();
            ctx.arc(p.x, p.y, pegRadius, 0, Math.PI * 2);
            ctx.fill();
        });
        ctx.shadowBlur = 0;
    }
    drawBoard();

    let initialSlots = drops[0].slots;
    const possibleMults = [2, 3, 5, 7, 10, 15, 20, 25, 50, "DOUBLE"];
    let shuffleInterval = setInterval(() => {
        for (let i = 0; i < 8; i++) {
            const r = possibleMults[Math.floor(Math.random() * possibleMults.length)];
            const slot = document.getElementById(`pk-slot-${i}`);
            slot.textContent = r === "DOUBLE" ? "DBL" : `x${r}`;
            slot.style.background = r === "DOUBLE" ? "linear-gradient(45deg, #ff0000, #ff7300)" : "rgba(30, 30, 56, 0.9)";
        }
    }, 100);

    setTimeout(() => {
        clearInterval(shuffleInterval);
        setSlots(initialSlots);
        playDrop(0);
    }, 2500);

    function setSlots(slotsArr) {
        for (let i = 0; i < 8; i++) {
            const slot = document.getElementById(`pk-slot-${i}`);
            const val = slotsArr[i];
            slot.textContent = val === "DOUBLE" ? "DBL" : `x${val}`;
            slot.style.background = val === "DOUBLE" ? "linear-gradient(45deg, #ff0000, #ff7300)" : "rgba(30, 30, 56, 0.9)";
            slot.style.transform = "scale(1.1)";
            setTimeout(() => { slot.style.transform = "scale(1)"; }, 300);
        }
    }

    function playDrop(dropIndex) {
        if (dropIndex >= drops.length) return;
        const dropData = drops[dropIndex];

        let dzHighlightInterval = setInterval(() => {
            document.querySelectorAll('.pk-dz').forEach(dz => dz.style.backgroundColor = 'rgba(255,255,255,0.1)');
            const rDz = Math.floor(Math.random() * 16);
            const dzEl = document.getElementById(`pk-dz-${rDz}`);
            if (dzEl) {
                dzEl.style.backgroundColor = '#00ffcc';
                dzEl.style.boxShadow = '0 0 10px #00ffcc';
            }
        }, 100);

        setTimeout(() => {
            clearInterval(dzHighlightInterval);
            document.querySelectorAll('.pk-dz').forEach(dz => {
                dz.style.backgroundColor = 'rgba(255,255,255,0.1)';
                dz.style.boxShadow = 'none';
            });
            const dz = document.getElementById(`pk-dz-${dropData.drop_zone}`);
            if (dz) {
                dz.style.backgroundColor = '#ff00ff';
                dz.style.boxShadow = '0 0 15px #ff00ff';
            }

            simulatePhysicsDrop(dropData, () => {
                const isDouble = dropData.landed_value === "DOUBLE";
                if (isDouble) {
                    const landedSlot = document.getElementById(`pk-slot-${dropData.landed_index}`);
                    if (landedSlot) {
                        landedSlot.style.transform = "scale(1.2)";
                        landedSlot.style.boxShadow = "0 0 20px red";
                    }
                    setTimeout(() => {
                        if (landedSlot) {
                            landedSlot.style.transform = "scale(1)";
                            landedSlot.style.boxShadow = "none";
                        }
                        if (dz) dz.style.backgroundColor = 'rgba(255,255,255,0.1)';
                        if (dropIndex + 1 < drops.length) {
                            setSlots(drops[dropIndex + 1].slots);
                            setTimeout(() => playDrop(dropIndex + 1), 1000);
                        }
                    }, 1500);
                } else {
                    const winnerSlot = document.getElementById(`pk-slot-${dropData.landed_index}`);
                    if (winnerSlot) winnerSlot.classList.add('winner-side');
                    showMinigameResultMultiplier(multiplier);
                    setTimeout(onComplete, 4000);
                }
            });
        }, 2000);
    }

    function simulatePhysicsDrop(dropData, onLanded) {
        const path = dropData.path;
        const spacingX = boardW / cols;
        const startX = (dropData.drop_zone + 0.5) * spacingX;
        let bx = startX;
        let by = -ballRadius;

        let currentStep = 0;
        let isAnimating = true;
        let targetX = bx;
        let targetY = boardH / (pegRows + 1.5);

        const stepDuration = 397; // 15% faster than 467
        let stepStartTime = performance.now();

        function animatePuck(now) {
            if (!isAnimating) return;
            const elapsed = now - stepStartTime;
            let progress = elapsed / stepDuration;

            if (progress >= 1) {
                currentStep++;
                if (currentStep > 15) {
                    isAnimating = false;
                    drawBoard();
                    onLanded();
                    return;
                }

                stepStartTime = now;
                bx = targetX;
                by = targetY;
                progress = 0;

                const dir = path[currentStep - 1] || (Math.random() > 0.5 ? 1 : -1);
                targetY = (currentStep + 1) * (boardH / (pegRows + 1.5));
                targetX = bx + (dir * spacingX / 2);
            }

            const easeX = progress;
            const easeY = progress;
            const bounceY = Math.sin(progress * Math.PI) * -15;

            const currX = bx + (targetX - bx) * easeX;
            const currY = by + (targetY - by) * easeY + bounceY;

            drawBoard();

            ctx.beginPath();
            ctx.arc(currX, currY, ballRadius, 0, Math.PI * 2);
            ctx.fillStyle = '#fff';
            ctx.fill();
            ctx.lineWidth = 2;
            ctx.strokeStyle = '#ff00ff';
            ctx.stroke();

            ctx.shadowColor = '#ff00ff';
            ctx.shadowBlur = 10;
            ctx.fill();
            ctx.shadowBlur = 0;

            requestAnimationFrame(animatePuck);
        }
        requestAnimationFrame(animatePuck);
    }
}


// ===== BALANCE =====
// Avviso di round annullato: banner temporaneo, senza bloccare il gioco —
// il nuovo dealer ha gia' aperto il round successivo.
function showRoundCancelled(round) {
    const banner = document.createElement('div');
    banner.className = 'round-cancelled-banner';
    banner.textContent = round
        ? `Round ${round} annullato: il dealer e' caduto. Le puntate non giocate sono state rimborsate.`
        : "Round annullato: il dealer e' caduto. Le puntate non giocate sono state rimborsate.";
    Object.assign(banner.style, {
        position: 'fixed', top: '20px', left: '50%', transform: 'translateX(-50%)',
        background: '#b8860b', color: '#fff', padding: '14px 22px', borderRadius: '8px',
        zIndex: '9999', fontWeight: 'bold', boxShadow: '0 4px 12px rgba(0,0,0,.4)'
    });
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 8000);
}

function fetchBalance() {
    if (!currentUser) return;
    authFetch(`/api/wallet/balance`)
        .then(r => r.json())
        .then(d => { if (d.success) updateBalanceDisplay(d.balance); })
        .catch(() => { });
}

// ===== DEV TOOLS =====
let activeDevSegment = null;

function clearDevSelection() {
    activeDevSegment = null;
    document.querySelectorAll('.dev-btn').forEach(btn => btn.classList.remove('active-dev-btn'));
}

// Il pannello di forzatura è riservato all'admin: l'endpoint risponde 403 a chiunque altro
function updateDevPanelVisibility() {
    const panel = document.querySelector('.dev-panel');
    if (panel) panel.style.display = (currentUser === 'admin') ? 'flex' : 'none';
}

function forceResult(segment, btnElement) {
    // Guardia difensiva: il pannello non è visibile ai non-admin, ma può essere mostrato a mano
    if (currentUser !== 'admin') return;

    // Ricliccare il segmento già attivo annulla la forzatura
    const isDeselect = (activeDevSegment === segment);
    const target = isDeselect ? 'NONE' : segment;

    authFetch(`/api/wallet/force-result?segment=${encodeURIComponent(target)}`, { method: 'POST' })
        .then(r => r.json())
        .then(d => {
            // Lo stato visivo segue la risposta del server invece di anticiparla
            if (!d.success) {
                showBetError(d.error || 'Impossibile forzare il segmento');
                return;
            }
            clearDevSelection();
            if (isDeselect) {
                console.log('[DEV] Forzatura annullata (esito casuale)');
                return;
            }
            activeDevSegment = segment;
            const btn = btnElement || document.querySelector(`.dev-btn[data-segment="${segment}"]`);
            if (btn) btn.classList.add('active-dev-btn');
            console.log(`[DEV] Prossimo segmento forzato: ${segment}`);
        })
        .catch(() => showBetError('Errore di connessione al server'));
}

// ===== BET SLIP INTERACTION LOGIC =====

chipHitboxes.forEach(chip => {
    chip.addEventListener('click', () => {
        const amount = parseFloat(chip.dataset.amount);
        if (amount > getAvailableBalance() + 0.001) {
            showBetError('Saldo insufficiente per questa fiche!');
            return;
        }
        chipHitboxes.forEach(c => c.classList.remove('active-chip'));
        chip.classList.add('active-chip');
        selectedChipAmount = amount;
    });
});

betHitboxes.forEach(betBox => {
    betBox.addEventListener('click', () => {
        if (currentPhase !== 'betting') {
            showBetError('Le scommesse sono chiuse!');
            return;
        }
        if (isBetSlipSubmitted) {
            showBetError('Schedina già inviata per questo round!');
            return;
        }

        const segment = betBox.dataset.segment;
        const amount = selectedChipAmount;

        if (!amount || amount <= 0) return;

        if (amount > getAvailableBalance() + 0.001) {
            showBetError('Saldo insufficiente!');
            return;
        }

        // Aggiunge localmente alla schedina (Bet Slip)
        if (!betSlip[segment]) betSlip[segment] = 0;
        betSlip[segment] = parseFloat((betSlip[segment] + amount).toFixed(2));
        
        totalBetThisRound = getBetSlipTotal();
        if (totalBetDisplay) totalBetDisplay.textContent = `$${totalBetThisRound.toFixed(2)}`;
        addChipToButton(betBox, betSlip[segment]);
        updateChipAvailability();
    });
});

// --- Bet on All (Central Buttons) Logic ---
const betGroups = {
    numbers: ['1', '2', '5', '10'],
    bonus: ['CoinFlip', 'Pachinko', 'CashHunt', 'CrazyTime']
};

betAllHitboxes.forEach(allBox => {
    allBox.addEventListener('click', () => {
        if (currentPhase !== 'betting') {
            showBetError('Le scommesse sono chiuse!');
            return;
        }
        if (isBetSlipSubmitted) {
            showBetError('Schedina già inviata per questo round!');
            return;
        }

        const groupKey = allBox.dataset.group;
        const segments = betGroups[groupKey];
        if (!segments) return;

        const amount = selectedChipAmount;
        if (!amount || amount <= 0) return;

        const totalNeeded = amount * segments.length;
        if (totalNeeded > getAvailableBalance() + 0.001) {
            showBetError('Saldo insufficiente per scommettere su tutti!');
            return;
        }

        for (const segment of segments) {
            if (!betSlip[segment]) betSlip[segment] = 0;
            betSlip[segment] = parseFloat((betSlip[segment] + amount).toFixed(2));
            const betBox = document.querySelector(`.bet-hitbox[data-segment="${segment}"]`);
            if (betBox) addChipToButton(betBox, betSlip[segment]);
        }

        totalBetThisRound = getBetSlipTotal();
        if (totalBetDisplay) totalBetDisplay.textContent = `$${totalBetThisRound.toFixed(2)}`;
        updateChipAvailability();
    });
});

// --- Undo Bets Logic (Reset locale del carrello) ---
const btnUndo = document.getElementById('btn-undo');
if (btnUndo) {
    btnUndo.addEventListener('click', () => {
        if (currentPhase !== 'betting') {
            showBetError('Le scommesse sono chiuse!');
            return;
        }
        if (isBetSlipSubmitted) {
            showBetError('Schedina già confermata per questo round!');
            return;
        }

        // Svuota lo stato locale della schedina
        betSlip = {};
        totalBetThisRound = 0;
        if (totalBetDisplay) totalBetDisplay.textContent = `$0.00`;

        // Rimuove graficamente le fiches impilate
        betHitboxes.forEach(hb => {
            const chip = hb.querySelector('.bet-stacked-chip');
            if (chip) chip.remove();
        });

        updateChipAvailability();
    });
}

// --- 2x Bets Logic (Raddoppio locale) ---
const btn2x = document.getElementById('btn-2x');
if (btn2x) {
    btn2x.addEventListener('click', () => {
        if (currentPhase !== 'betting') {
            showBetError('Le scommesse sono chiuse!');
            return;
        }
        if (isBetSlipSubmitted) {
            showBetError('Schedina già inviata per questo round!');
            return;
        }

        const currentTotal = getBetSlipTotal();
        if (currentTotal <= 0) return;

        if (currentTotal > getAvailableBalance() + 0.001) {
            showBetError('Saldo insufficiente per raddoppiare!');
            return;
        }

        for (const segment of Object.keys(betSlip)) {
            betSlip[segment] = parseFloat((betSlip[segment] * 2).toFixed(2));
            const betBox = document.querySelector(`.bet-hitbox[data-segment="${segment}"]`);
            if (betBox) addChipToButton(betBox, betSlip[segment]);
        }

        totalBetThisRound = getBetSlipTotal();
        if (totalBetDisplay) totalBetDisplay.textContent = `$${totalBetThisRound.toFixed(2)}`;
        updateChipAvailability();
    });
}

// --- Confirm Bet Slip Button (Piazza Scommesse) ---
const btnConfirmBet = document.getElementById('btn-confirm-bet');
if (btnConfirmBet) {
    btnConfirmBet.addEventListener('click', () => {
        if (currentPhase !== 'betting') {
            showBetError('Le scommesse sono chiuse!');
            return;
        }
        if (isBetSlipSubmitted) {
            showBetError('Schedina già inviata!');
            return;
        }
        if (getBetSlipTotal() <= 0) {
            showBetError('Seleziona almeno una scommessa prima di confermare!');
            return;
        }
        submitBetSlip();
    });
}

function addChipToButton(hitBox, totalAmount) {
    let chip = hitBox.querySelector('.bet-stacked-chip');
    if (!chip) {
        chip = document.createElement('div');
        chip.className = 'bet-stacked-chip';
        hitBox.appendChild(chip);
    }
    chip.textContent = totalAmount % 1 === 0 ? totalAmount : totalAmount.toFixed(1);
}

function clearChips() {
    document.querySelectorAll('.bet-stacked-chip').forEach(c => c.remove());
    totalBetThisRound = 0;
    if (totalBetDisplay) totalBetDisplay.textContent = `$0.00`;
}

function showBetError(msg) {
    const el = document.createElement('div');
    el.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:rgba(239,68,68,0.9);color:white;padding:10px 24px;border-radius:8px;font-weight:600;z-index:9999;animation:fadeIn 0.3s ease;';
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 2500);
}

/* =========================================================================
   CRAZY TIME ANIMATION
   ========================================================================= */
function animateCrazyTime(multiplier, details, onComplete) {
    const overlay = document.getElementById('minigame-overlay');
    const goldenFlash = document.getElementById('golden-flash');
    
    // Add golden flash animation
    goldenFlash.classList.add('flash-active');
    
    // Wait for the flash to cover the screen before showing the wheel
    setTimeout(() => {
        // Show crazytime layout
        document.body.classList.add('crazytime-active');
        overlay.classList.add('visible'); // Show the minigame layer
        
        // Inject HTML
        minigameArea.innerHTML = `
            <div class="ct-wheel-container" id="ct-wheel-container">
                <div class="ct-flappers">
                    <div class="ct-flapper green" id="ct-flapper-green"></div>
                    <div class="ct-flapper blue" id="ct-flapper-blue"></div>
                    <div class="ct-flapper yellow" id="ct-flapper-yellow"></div>
                </div>
                <img src="img/minigame_crazy_time.png?v=99" class="ct-wheel-img" id="ct-wheel-img" alt="Crazy Time Wheel">
            </div>
            
            <div class="ct-popup" id="ct-popup">
                <h2>Scegli il tuo Flapper</h2>
                <div class="ct-popup-timer" id="ct-timer">5</div>
                <div class="ct-popup-choices">
                    <div class="ct-choice-btn green" data-color="green"></div>
                    <div class="ct-choice-btn blue" data-color="blue"></div>
                    <div class="ct-choice-btn yellow" data-color="yellow"></div>
                </div>
            </div>
        `;

        const wheelImg = document.getElementById('ct-wheel-img');
        const timerEl = document.getElementById('ct-timer');
        const popup = document.getElementById('ct-popup');
        const choiceBtns = document.querySelectorAll('.ct-choice-btn');
        
        let selectedFlapper = 'blue'; // default
        let timeLeft = 5;
        let timerInt;
        let choiceSent = false;

        // Handle clicks on popup
        choiceBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                if (timeLeft > 0) {
                    selectedFlapper = btn.dataset.color;
                    choiceBtns.forEach(b => {
                        b.style.opacity = '0.3';
                        b.style.transform = 'scale(0.9)';
                    });
                    btn.style.opacity = '1';
                    btn.style.transform = 'scale(1.1)';

                    // Invia la scelta al server IMMEDIATAMENTE
                    if (!choiceSent && currentUser) {
                        choiceSent = true;
                        authFetch(`/api/game/choice`, {
                            method: 'POST',
                            body: {minigame: 'CrazyTime', choice: selectedFlapper}
                        }).catch(e => console.error("Errore invio scelta", e));
                    }
                }
            });
        });

        // Start countdown
        timerInt = setInterval(() => {
            timeLeft--;
            if (timeLeft > 0) {
                timerEl.textContent = timeLeft;
            } else {
                finishSelection();
            }
        }, 1000);

        let selectionDone = false;
        function finishSelection() {
            if(selectionDone) return;
            selectionDone = true;
            clearInterval(timerInt);
            popup.style.display = 'none';

            // Send choice to backend
            if (!choiceSent && currentUser) {
                choiceSent = true;
                authFetch(`/api/game/choice`, {
                    method: 'POST',
                    body: {minigame: 'CrazyTime', choice: selectedFlapper}
                }).catch(e => console.error("Errore invio scelta", e));
            }

            // Highlight chosen flapper
            document.getElementById(`ct-flapper-${selectedFlapper}`).classList.add('selected');
            
            setTimeout(() => {
                startSpin();
            }, 500); // slight delay before spin
        }

        function startSpin() {
            const winnerIndex = details.winner_index !== undefined ? details.winner_index : 0;
            const segmentAngle = 360 / 64; 
            
            let targetAngle = -(winnerIndex * segmentAngle);
            targetAngle -= (360 * 5); // 5 giri completi
            
            // Random offset all'interno del segmento
            const randomOffset = (Math.random() - 0.5) * (segmentAngle * 0.8);
            targetAngle += randomOffset;

            wheelImg.style.transform = `rotate(${targetAngle}deg)`;

            // Wait for spin to finish (10.5s transition in CSS)
            setTimeout(() => {
                let finalMultiplier = details[`${selectedFlapper}_multiplier`] || multiplier;
                showMinigameResultMultiplier(finalMultiplier);
                
                // End of game: Trigger flash again to restore
                goldenFlash.classList.remove('flash-active');
                void goldenFlash.offsetWidth; // force reflow
                goldenFlash.classList.add('flash-active');

                setTimeout(() => {
                    document.body.classList.remove('crazytime-active');
                    overlay.classList.remove('visible');
                    minigameArea.innerHTML = ''; // Cleanup
                    goldenFlash.classList.remove('flash-active');
                    onComplete(finalMultiplier);
                }, 750); // Restore exactly when flash is totally white

            }, 10500);
        }

    }, 750); // The flash reaches its peak at ~750ms (50% of 1.5s)
}

console.log("App.js caricato con successo!");

// ===== SIDE PANEL LOGIC =====
const menuToggleBtn = document.getElementById('menu-toggle-btn');
const closeMenuBtn = document.getElementById('close-menu-btn');
const sidePanel = document.getElementById('side-panel');
const logoutBtn = document.getElementById('logout-btn');

if (menuToggleBtn && closeMenuBtn && sidePanel) {
    menuToggleBtn.addEventListener('click', () => {
        sidePanel.classList.add('open');
    });

    closeMenuBtn.addEventListener('click', () => {
        sidePanel.classList.remove('open');
    });
}

if (logoutBtn) {
    logoutBtn.addEventListener('click', async () => {
        try {
            await authFetch('/api/auth/logout', { method: 'POST' });
        } catch (e) { /* ignore errors on logout */ }
        
        // Disconnect STOMP
        if (stompClient) {
            stompClient.disconnect();
            stompClient = null;
        }
        // Clear session
        sessionStorage.removeItem('token');
        currentUser = null;
        shouldReconnect = false;
        
        // Hide panel & game screen, show auth screen
        if (sidePanel) sidePanel.classList.remove('open');
        const gameScreen = document.getElementById('game-screen');
        const authScreen = document.getElementById('auth-screen');
        if (gameScreen) gameScreen.style.display = 'none';
        if (authScreen) authScreen.style.display = 'block';
        const authForm = document.getElementById('auth-form');
        if (authForm) authForm.reset();
        
        console.log("Logout effettuato");
    });
}

// ===== BET HISTORY LOGIC =====
const historyBtn = document.getElementById('history-btn');
const historyOverlay = document.getElementById('history-overlay');
const closeHistoryBtn = document.getElementById('close-history-btn');
const historyList = document.getElementById('history-list');
const historySummary = document.getElementById('history-summary');
const historyFilterBtns = document.querySelectorAll('.history-filter-btn');

let allBetsData = [];
let currentHistoryFilter = 'all';

const SEGMENT_DISPLAY_NAMES = {
    '1': 'Numero 1',
    '2': 'Numero 2',
    '5': 'Numero 5',
    '10': 'Numero 10',
    'Pachinko': 'Pachinko',
    'CoinFlip': 'Coin Flip',
    'CashHunt': 'Cash Hunt',
    'CrazyTime': 'Crazy Time'
};

const STATUS_LABELS = {
    'WON': 'Vinta',
    'LOST': 'Persa',
    'PENDING': 'In Corso',
    'REFUNDED': 'Rimborsata'
};

function formatHistoryDate(isoStr) {
    try {
        const d = new Date(isoStr);
        const now = new Date();
        const diffMs = now - d;
        const diffMin = Math.floor(diffMs / 60000);
        const diffHrs = Math.floor(diffMs / 3600000);

        const time = d.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' });
        
        if (diffMin < 1) return 'Adesso';
        if (diffMin < 60) return `${diffMin} min fa`;
        if (diffHrs < 24) return `${diffHrs}h fa — ${time}`;
        
        const date = d.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit' });
        return `${date} — ${time}`;
    } catch (e) {
        return isoStr;
    }
}

function groupBetsByRound(bets) {
    const groups = [];
    let currentGroup = null;

    for (const bet of bets) {
        const betRound = bet.round && bet.round > 0 ? bet.round : null;
        const betTime = new Date(bet.timestamp).getTime();

        let isSameGroup = false;
        if (currentGroup) {
            if (betRound !== null && currentGroup.round !== null) {
                isSameGroup = (betRound === currentGroup.round);
            } else {
                // Se il round non è disponibile, raggruppa per prossimità temporale (< 6s)
                const firstTime = new Date(currentGroup.bets[0].timestamp).getTime();
                isSameGroup = Math.abs(betTime - firstTime) < 6000;
            }
        }

        if (isSameGroup) {
            currentGroup.bets.push(bet);
            currentGroup.totalAmount += (parseFloat(bet.amount) || 0);
            if (bet.status === 'WON') {
                currentGroup.totalPayout += (parseFloat(bet.payout) || 0);
            }
        } else {
            currentGroup = {
                round: betRound,
                timestamp: bet.timestamp,
                bets: [bet],
                totalAmount: (parseFloat(bet.amount) || 0),
                totalPayout: bet.status === 'WON' ? (parseFloat(bet.payout) || 0) : 0
            };
            groups.push(currentGroup);
        }
    }
    return groups;
}

function renderHistoryCards(bets) {
    if (!bets || bets.length === 0) {
        historyList.innerHTML = `
            <div class="history-empty">
                <span class="empty-icon">🎰</span>
                <span class="empty-text">Nessuna puntata trovata</span>
            </div>`;
        return;
    }

    const groups = groupBetsByRound(bets);

    historyList.innerHTML = groups.map((group, gIdx) => {
        const roundTitle = group.round ? `Round #${group.round}` : `Giocata`;
        const countText = group.bets.length === 1 ? '1 puntata' : `${group.bets.length} puntate`;
        const totalRoundStaked = `$${group.totalAmount.toFixed(2)}`;

        const cardsHtml = group.bets.map((bet, i) => {
            const segColor = SEGMENT_COLORS[bet.segment] || '#64748b';
            const segLabel = SEGMENT_TEXT_SHORT[bet.segment] || bet.segment;
            const segName = SEGMENT_DISPLAY_NAMES[bet.segment] || bet.segment;
            const status = bet.status || 'PENDING';
            const statusLabel = STATUS_LABELS[status] || status;
            const payout = parseFloat(bet.payout) || 0;
            const amount = parseFloat(bet.amount) || 0;

            let payoutClass = 'pending';
            let payoutText = 'In attesa...';
            if (status === 'WON') {
                payoutClass = 'won';
                payoutText = `+$${payout.toFixed(2)}`;
            } else if (status === 'LOST') {
                payoutClass = 'lost';
                payoutText = `-$${amount.toFixed(2)}`;
            } else if (status === 'REFUNDED') {
                payoutClass = 'refunded';
                payoutText = `↩ $${amount.toFixed(2)}`;
            }

            return `
                <div class="history-card" style="animation-delay: ${(gIdx * 2 + i) * 0.03}s">
                    <div class="history-card-segment" style="background: ${segColor}">${segLabel}</div>
                    <div class="history-card-info">
                        <span class="segment-name">${segName}</span>
                    </div>
                    <div class="history-card-amounts">
                        <span class="bet-amount">$${amount.toFixed(2)}</span>
                        <span class="bet-payout ${payoutClass}">${payoutText}</span>
                        <span class="history-status-badge ${status}">${statusLabel}</span>
                    </div>
                </div>`;
        }).join('');

        return `
            <div class="history-round-group" style="animation-delay: ${gIdx * 0.04}s">
                <div class="history-round-header">
                    <div class="history-round-title">
                        <span class="history-round-badge">🎯 ${roundTitle}</span>
                        <span class="history-round-count">(${countText} • Totale ${totalRoundStaked})</span>
                    </div>
                    <div class="history-round-meta">
                        <span class="history-round-time">${formatHistoryDate(group.timestamp)}</span>
                    </div>
                </div>
                <div class="history-round-cards">
                    ${cardsHtml}
                </div>
            </div>`;
    }).join('');
}

function renderHistorySummary(bets) {
    if (!bets || bets.length === 0) {
        historySummary.innerHTML = '';
        return;
    }

    let totalBet = 0, totalWon = 0, totalLost = 0;
    bets.forEach(b => {
        const amount = parseFloat(b.amount) || 0;
        const payout = parseFloat(b.payout) || 0;
        totalBet += amount;
        if (b.status === 'WON') totalWon += payout;
        if (b.status === 'LOST') totalLost += amount;
    });
    const net = totalWon - totalLost;

    historySummary.innerHTML = `
        <div class="history-summary-item">
            <span class="summary-label">Puntato</span>
            <span class="summary-value total-bet">$${totalBet.toFixed(2)}</span>
        </div>
        <div class="history-summary-item">
            <span class="summary-label">Vinto</span>
            <span class="summary-value total-won">$${totalWon.toFixed(2)}</span>
        </div>
        <div class="history-summary-item">
            <span class="summary-label">Perso</span>
            <span class="summary-value total-lost">$${totalLost.toFixed(2)}</span>
        </div>
        <div class="history-summary-item">
            <span class="summary-label">Netto</span>
            <span class="summary-value net-result">${net >= 0 ? '+' : ''}$${net.toFixed(2)}</span>
        </div>`;
}

function applyHistoryFilter(filter) {
    currentHistoryFilter = filter;
    const filtered = filter === 'all' ? allBetsData : allBetsData.filter(b => b.status === filter);
    renderHistoryCards(filtered);
    renderHistorySummary(filtered);
}

async function loadBetHistory() {
    historyList.innerHTML = `
        <div class="history-loading">
            <div class="spinner"></div>
            <p>Caricamento storico...</p>
        </div>`;
    historySummary.innerHTML = '';

    try {
        const res = await authFetch('/api/wallet/history');
        if (!res.ok) throw new Error('Errore nel caricamento');
        const data = await res.json();
        
        if (data.success && data.bets) {
            allBetsData = data.bets;
            applyHistoryFilter(currentHistoryFilter);
        } else {
            historyList.innerHTML = `
                <div class="history-empty">
                    <span class="empty-icon">⚠️</span>
                    <span class="empty-text">Errore nel caricamento dello storico</span>
                </div>`;
        }
    } catch (err) {
        console.error('Errore caricamento storico:', err);
        historyList.innerHTML = `
            <div class="history-empty">
                <span class="empty-icon">⚠️</span>
                <span class="empty-text">Impossibile caricare lo storico</span>
            </div>`;
    }
}

// Open history overlay
if (historyBtn && historyOverlay) {
    historyBtn.addEventListener('click', () => {
        sidePanel.classList.remove('open');
        historyOverlay.classList.add('open');
        currentHistoryFilter = 'all';
        historyFilterBtns.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.filter === 'all');
        });
        loadBetHistory();
    });
}

// Close history overlay
if (closeHistoryBtn && historyOverlay) {
    closeHistoryBtn.addEventListener('click', () => {
        historyOverlay.classList.remove('open');
    });
}

// Close on background click
if (historyOverlay) {
    historyOverlay.addEventListener('click', (e) => {
        if (e.target === historyOverlay) {
            historyOverlay.classList.remove('open');
        }
    });
}

// Filter tabs
historyFilterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        historyFilterBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        applyHistoryFilter(btn.dataset.filter);
    });
});

