// =========================================================
//  DISTRIBUTED CRAZY TIME — app.js
//  Canvas wheel, 10s spin, animated minigames, auth flow
// =========================================================

// ===== CONSTANTS =====
const SEGMENTS = [
    "1","2","1","5","1","2","1","Pachinko","1","2",
    "1","10","2","1","5","1","2","CoinFlip","1","2",
    "1","5","1","2","10","1","2","CashHunt","1","2",
    "1","5","1","Pachinko","2","1","10","1","2","1",
    "5","2","1","Pachinko","1","2","1","10","2","5",
    "1","CoinFlip","1","CrazyTime"
];
const NUM_SEGMENTS = 54;

const SEGMENT_COLORS = {
    "1":         "#2563eb",
    "2":         "#eab308",
    "5":         "#e844a0",
    "10":        "#8b5cf6",
    "Pachinko":  "#d946ef",
    "CoinFlip":  "#ef4444",
    "CashHunt":  "#10b981",
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
let myBetsThisRound = {};
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
const logoutBtn = document.getElementById('logout-btn');
const betAllBtn = document.getElementById('bet-all-btn');

const playerName = document.getElementById('player-name');
const playerBalance = document.getElementById('player-balance');
const phaseText = document.getElementById('phase-text');
const timerDisplay = document.getElementById('timer-display');
const roundDisplay = document.getElementById('round-display');

const wheelCanvas = document.getElementById('wheel-canvas');
const wheelCenterText = document.getElementById('wheel-center-text');
const lastResultsContainer = document.getElementById('last-results');

const betButtons = document.querySelectorAll('.bet-btn');
const betAmountInput = document.getElementById('bet-amount');

const resultOverlay = document.getElementById('result-overlay');
const resultTitle = document.getElementById('result-title');
const resultDesc = document.getElementById('result-desc');
const resultMultiplier = document.getElementById('result-multiplier');

const minigameOverlay = document.getElementById('minigame-overlay');
const minigameTitle = document.getElementById('minigame-title');
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
    const radius = size / 2 - 6;
    const arcAngle = (2 * Math.PI) / NUM_SEGMENTS;

    ctx.clearRect(0, 0, size, size);
    ctx.save();
    ctx.translate(cx, cy);
    ctx.rotate(rotation);

    for (let i = 0; i < NUM_SEGMENTS; i++) {
        const seg = SEGMENTS[i];
        const startAngle = i * arcAngle - Math.PI / 2;
        const endAngle = startAngle + arcAngle;

        // Draw arc segment
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.arc(0, 0, radius, startAngle, endAngle);
        ctx.closePath();
        ctx.fillStyle = SEGMENT_COLORS[seg];
        ctx.fill();

        // Segment border
        ctx.strokeStyle = 'rgba(0,0,0,0.3)';
        ctx.lineWidth = 1;
        ctx.stroke();

        // Text
        ctx.save();
        const textAngle = startAngle + arcAngle / 2;
        ctx.rotate(textAngle);
        ctx.translate(radius * 0.72, 0);
        ctx.rotate(Math.PI / 2);
        ctx.fillStyle = '#ffffff';
        ctx.font = 'bold 10px Outfit';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.shadowColor = 'rgba(0,0,0,0.7)';
        ctx.shadowBlur = 3;
        ctx.fillText(SEGMENT_TEXT_SHORT[seg] || seg, 0, 0);
        ctx.shadowBlur = 0;
        ctx.restore();
    }

    // Outer ring
    ctx.beginPath();
    ctx.arc(0, 0, radius, 0, Math.PI * 2);
    ctx.strokeStyle = 'rgba(255,255,255,0.15)';
    ctx.lineWidth = 4;
    ctx.stroke();

    // Inner ring (behind center overlay)
    ctx.beginPath();
    ctx.arc(0, 0, 38, 0, Math.PI * 2);
    ctx.fillStyle = '#0d1020';
    ctx.fill();
    ctx.strokeStyle = 'rgba(255,255,255,0.1)';
    ctx.lineWidth = 2;
    ctx.stroke();

    ctx.restore();
}

// Initial draw
drawWheel(0);

// ===== SPIN ANIMATION =====
function spinWheel(targetIndex, duration, onComplete) {
    if (isSpinning) return;
    isSpinning = true;

    const arcAngle = (2 * Math.PI) / NUM_SEGMENTS;
    // The pointer is at the top (12 o'clock = -π/2).
    // We want the target segment center to align with the pointer.
    // Segment i center is at: i * arcAngle + arcAngle/2 (from -π/2 in draw)
    // So we need rotation = -(targetIndex * arcAngle + arcAngle/2)
    // Plus several full rotations for visual effect
    const targetAngle = -(targetIndex * arcAngle + arcAngle / 2);
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
        const res = await fetch(`/api/auth/login?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`, { method: 'POST' });
        const data = await res.json();
        if (data.success) {
            currentUser = data.username;
            currentBalance = data.balance;
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
        const res = await fetch(`/api/wallet/register?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}&initialBalance=${initialBalance}`, { method: 'POST' });
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

logoutBtn.addEventListener('click', async () => {
    if (!currentUser) return;
    try {
        await fetch(`/api/auth/logout?username=${encodeURIComponent(currentUser)}`, { method: 'POST' });
    } catch (e) { /* ignore */ }

    if (stompClient) { stompClient.disconnect(); stompClient = null; }

    currentUser = null;
    myBetsThisRound = {};
    authScreen.style.display = 'block';
    gameScreen.style.display = 'none';
    document.getElementById('username').value = '';
    document.getElementById('password').value = '';
    authError.textContent = '';
});

// ===== GAME START =====
function startGame() {
    authScreen.style.display = 'none';
    gameScreen.style.display = 'flex';
    playerName.textContent = currentUser;
    updateBalanceDisplay(currentBalance);
    connectWebSocket();

    fetch('/api/game/state')
        .then(res => res.json())
        .then(data => {
            if (data.success) handleGameState(data);
        })
        .catch(() => {});
}

function updateBalanceDisplay(amount) {
    currentBalance = parseFloat(amount);
    playerBalance.textContent = `$${currentBalance.toFixed(2)}`;
}

// ===== WEBSOCKET =====
function connectWebSocket() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null;

    stompClient.connect({}, function () {
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

function handleGameState(data) {
    const phase = data.phase;
    roundDisplay.textContent = `Round #${data.round || 1}`;

    if (phase === 'betting') {
        currentPhase = 'betting';
        phaseText.textContent = 'PLACE YOUR BETS';
        phaseText.className = 'timer-phase betting';
        timerDisplay.textContent = data.time_left;

        if (data.time_left <= 3) {
            timerDisplay.classList.add('warning');
        } else {
            timerDisplay.classList.remove('warning');
        }

        betButtons.forEach(b => b.disabled = false);

        // Reset round on fresh betting phase
        if (data.time_left >= 9) {
            clearChips();
            myBetsThisRound = {};
            wheelCenterText.innerHTML = 'CRAZY<br>TIME';
            resultOverlay.style.display = 'none';
            minigameOverlay.style.display = 'none';
        }

    } else if (phase === 'spinning') {
        currentPhase = 'spinning';
        phaseText.textContent = 'NO MORE BETS';
        phaseText.className = 'timer-phase spinning';
        timerDisplay.textContent = '🎰';
        timerDisplay.classList.remove('warning');
        betButtons.forEach(b => b.disabled = true);

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
        timerDisplay.textContent = '🎪';
        betButtons.forEach(b => b.disabled = true);

        // Show minigame overlay (animation will be triggered by handleGameResult)
        if (minigameOverlay.style.display === 'none') {
            showMinigameWaiting(data.minigame);
        }
    }
}

// ===== GAME RESULT HANDLER =====
function handleGameResult(data) {
    wheelCenterText.innerHTML = data.winner || '?';

    // Add to last results
    addLastResult(data.winner, data.multiplier);

    // Calculate if player won
    const myBetAmount = myBetsThisRound[data.winner];
    const isWin = myBetAmount !== undefined;
    let winAmount = 0;
    if (isWin) {
        winAmount = myBetAmount * data.multiplier;
    }

    const isMinigame = data.result_type === 'minigame';
    const details = data.details || {};

    if (isMinigame && data.winner) {
        // Show minigame animation first, then result
        showMinigameAnimation(data.winner, data.multiplier, details, () => {
            showResult(isWin, data.winner, data.multiplier, winAmount);
            if (isWin) fetchBalance();
        });
    } else {
        // Direct multiplier — show result immediately after spin
        const delay = isSpinning ? 500 : 100;
        setTimeout(() => {
            minigameOverlay.style.display = 'none';
            showResult(isWin, data.winner, data.multiplier, winAmount);
            if (isWin) fetchBalance();
        }, delay);
    }
}

function showResult(isWin, winner, multiplier, winAmount) {
    if (!isWin) {
        minigameOverlay.style.display = 'none';
        return;
    }

    resultOverlay.style.display = 'flex';
    resultOverlay.style.zIndex = '1000'; // Make sure it sits above minigame
    resultTitle.textContent = 'VITTORIA!';
    resultTitle.className = 'result-title win';
    resultDesc.textContent = `Hai puntato su ${winner}`;
    resultMultiplier.textContent = `x${multiplier} → +$${winAmount.toFixed(2)}`;
    resultMultiplier.className = 'result-multiplier win-amount';

    // Auto-hide after 5 seconds
    setTimeout(() => {
        resultOverlay.style.display = 'none';
        minigameOverlay.style.display = 'none'; // Hide minigame overlay when win is hidden
    }, 5000);
}

function addLastResult(winner, multiplier) {
    lastResults.unshift({ winner, multiplier });
    if (lastResults.length > 10) lastResults.pop();
    renderLastResults();
}

function renderLastResults() {
    lastResultsContainer.innerHTML = lastResults.map(r => {
        const color = SEGMENT_COLORS[r.winner] || '#666';
        return `<div class="last-result-chip" style="background:${color}">${r.winner} x${r.multiplier}</div>`;
    }).join('');
}

// ===== MINIGAME ANIMATIONS =====

function showMinigameWaiting(name) {
    minigameOverlay.style.display = 'flex';
    minigameTitle.textContent = name.toUpperCase();
    minigameArea.innerHTML = `
        <div style="font-size:3rem;margin-bottom:16px;">${getMinigameEmoji(name)}</div>
        <div style="font-size:1.2rem;color:var(--text-dim);animation:pulse 1s infinite alternate;">
            Preparazione in corso...
        </div>
    `;
}

function getMinigameEmoji(name) {
    const emojis = { Pachinko: '🔴', CoinFlip: '🪙', CashHunt: '🎯', CrazyTime: '🎡' };
    return emojis[name] || '🎰';
}

function showMinigameAnimation(name, multiplier, details, onComplete) {
    minigameOverlay.style.display = 'flex';
    minigameTitle.textContent = name.toUpperCase();

    switch (name) {
        case 'Pachinko':
            animatePachinko(multiplier, details, onComplete);
            break;
        case 'CoinFlip':
            animateCoinFlip(multiplier, details, onComplete);
            break;
        case 'CashHunt':
            animateCashHunt(multiplier, details, onComplete);
            break;
        case 'CrazyTime':
            animateCrazyTime(multiplier, details, onComplete);
            break;
        default:
            setTimeout(onComplete, 2000);
    }
}

function showMinigameResultMultiplier(multiplier) {
    const res = document.createElement('div');
    res.style.cssText = 'position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:5rem;font-weight:900;color:#fbbf24;text-shadow:0 0 30px rgba(0,0,0,0.9);z-index:100;animation:popIn 0.5s ease;background:rgba(0,0,0,0.6);padding:20px 40px;border-radius:20px;border:3px solid #fbbf24;';
    res.textContent = `x${multiplier}`;
    minigameArea.appendChild(res);
}

// --- PACHINKO ---
function animatePachinko(multiplier, details, onComplete) {
    const slots = details.slots || [2, 3, 5, 8, 10, 15, 25, 50, 100];
    const slotIndex = details.slot_index !== undefined ? details.slot_index : 0;

    // Build board
    let html = '<div class="pachinko-board">';
    html += '<div class="pachinko-pegs">';
    for (let row = 0; row < 6; row++) {
        const pegsInRow = row % 2 === 0 ? 7 : 6;
        html += '<div class="peg-row">';
        for (let p = 0; p < pegsInRow; p++) {
            html += `<div class="peg" data-row="${row}" data-col="${p}"></div>`;
        }
        html += '</div>';
    }
    html += '</div>';

    // Ball
    html += '<div class="pachinko-ball" id="pachinko-ball" style="top:0;left:50%;transform:translate(-50%,0);"></div>';

    // Slots
    html += '<div class="pachinko-slots">';
    slots.forEach((s, i) => {
        html += `<div class="pachinko-slot" data-index="${i}">x${s}</div>`;
    });
    html += '</div></div>';

    minigameArea.innerHTML = html;

    // Animate ball falling
    const ball = document.getElementById('pachinko-ball');
    const board = minigameArea.querySelector('.pachinko-board');
    const pegRows = board.querySelectorAll('.peg-row');
    let step = 0;

    function dropStep() {
        if (step < pegRows.length) {
            const row = pegRows[step];
            const pegs = row.querySelectorAll('.peg');
            const pegIdx = Math.min(Math.floor(Math.random() * pegs.length), pegs.length - 1);
            const peg = pegs[pegIdx];

            // Highlight peg
            peg.classList.add('hit');

            // Move ball
            const rect = peg.getBoundingClientRect();
            const boardRect = board.getBoundingClientRect();
            ball.style.top = (rect.top - boardRect.top + 5) + 'px';
            ball.style.left = (rect.left - boardRect.left + 5) + 'px';
            ball.style.transform = 'none';

            step++;
            setTimeout(dropStep, 800);
        } else {
            // Ball reaches slot
            const slotEls = board.querySelectorAll('.pachinko-slot');
            if (slotEls[slotIndex]) {
                const slotEl = slotEls[slotIndex];
                const slotRect = slotEl.getBoundingClientRect();
                const boardRect = board.getBoundingClientRect();
                ball.style.top = (slotRect.top - boardRect.top - 10) + 'px';
                ball.style.left = (slotRect.left - boardRect.left + slotRect.width / 2 - 8) + 'px';

                setTimeout(() => {
                    slotEl.classList.add('winner');
                    showMinigameResultMultiplier(multiplier);
                    setTimeout(onComplete, 4000);
                }, 800);
            } else {
                showMinigameResultMultiplier(multiplier);
                setTimeout(onComplete, 4000);
            }
        }
    }

    setTimeout(dropStep, 800);
}

// --- COIN FLIP ---
function animateCoinFlip(multiplier, details, onComplete) {
    const sideA = details.side_a || 5;
    const sideB = details.side_b || 10;
    const winnerSide = details.winner_side || 'heads';

    let html = `
        <div class="coinflip-scene">
            <div class="coinflip-coin" id="cf-coin">
                <div class="coin-side coin-heads">
                    <span>x${sideA}</span>
                    <span class="coin-label">HEADS</span>
                </div>
                <div class="coin-side coin-tails">
                    <span>x${sideB}</span>
                    <span class="coin-label">TAILS</span>
                </div>
            </div>
        </div>
        <div class="coinflip-sides-display">
            <div class="side-display" id="cf-side-a">
                <div class="side-label">HEADS</div>
                <div class="side-mult">x${sideA}</div>
            </div>
            <div class="side-display" id="cf-side-b">
                <div class="side-label">TAILS</div>
                <div class="side-mult">x${sideB}</div>
            </div>
        </div>
    `;

    minigameArea.innerHTML = html;

    const coin = document.getElementById('cf-coin');

    // Start flipping
    setTimeout(() => {
        coin.classList.add('flipping');

        setTimeout(() => {
            coin.classList.remove('flipping');
            coin.classList.add(winnerSide === 'heads' ? 'heads' : 'tails');

            // Highlight winner side
            const winnerId = winnerSide === 'heads' ? 'cf-side-a' : 'cf-side-b';
            document.getElementById(winnerId).classList.add('winner-side');

            showMinigameResultMultiplier(multiplier);
            setTimeout(onComplete, 4000);
        }, 2500);
    }, 1000);
}

// --- CASH HUNT ---
function animateCashHunt(multiplier, details, onComplete) {
    const grid = details.grid || [5,10,15,2,3,8,20,50,25,10,5,3,2,15,100,75];
    const cellIndex = details.cell_index !== undefined ? details.cell_index : 0;

    let html = '<div class="cashhunt-grid">';
    for (let i = 0; i < 16; i++) {
        html += `
            <div class="cashhunt-card" data-index="${i}" id="ch-card-${i}">
                <div class="cashhunt-card-inner">
                    <div class="cashhunt-card-front">?</div>
                    <div class="cashhunt-card-back">x${grid[i]}</div>
                </div>
            </div>
        `;
    }
    html += '</div>';

    minigameArea.innerHTML = html;

    // Shuffle animation — briefly show all, then hide
    setTimeout(() => {
        // Reveal all briefly
        document.querySelectorAll('.cashhunt-card').forEach(c => c.classList.add('revealed'));

        setTimeout(() => {
            // Hide all again
            document.querySelectorAll('.cashhunt-card').forEach(c => c.classList.remove('revealed'));

            setTimeout(() => {
                // Reveal winner
                const winnerCard = document.getElementById(`ch-card-${cellIndex}`);
                if (winnerCard) {
                    winnerCard.classList.add('revealed', 'winner');
                }

                // Reveal all others after a beat
                setTimeout(() => {
                    document.querySelectorAll('.cashhunt-card').forEach(c => {
                        c.classList.add('revealed');
                    });
                    
                    showMinigameResultMultiplier(multiplier);
                    setTimeout(onComplete, 4000);
                }, 2000);
            }, 2500);
        }, 3000);
    }, 1000);
}

// --- CRAZY TIME BONUS WHEEL ---
function animateCrazyTime(multiplier, details, onComplete) {
    const segments = details.segments || [5, 10, 15, 20, 25, 50, 100, 200];
    const winnerIndex = details.winner_index !== undefined ? details.winner_index : 0;
    const boost = details.boost || 'none';
    const baseMult = details.base_multiplier || multiplier;

    let html = `
        <div class="crazytime-wheel-container">
            <div class="crazytime-pointer">▼</div>
            <canvas id="crazytime-canvas" width="280" height="280"></canvas>
        </div>
        <div id="ct-result" style="font-size:2rem;font-weight:900;color:white;margin-top:12px;"></div>
    `;

    minigameArea.innerHTML = html;

    const ctCanvas = document.getElementById('crazytime-canvas');
    const ctCtx = ctCanvas.getContext('2d');
    const ctColors = ['#ef4444', '#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316'];
    const numSegs = segments.length;
    const arcAngle = (2 * Math.PI) / numSegs;

    function drawCTWheel(rot) {
        const dpr = window.devicePixelRatio || 1;
        ctCanvas.width = 280 * dpr;
        ctCanvas.height = 280 * dpr;
        ctCtx.scale(dpr, dpr);
        const cx = 140, cy = 140, r = 130;

        ctCtx.clearRect(0, 0, 280, 280);
        ctCtx.save();
        ctCtx.translate(cx, cy);
        ctCtx.rotate(rot);

        for (let i = 0; i < numSegs; i++) {
            const start = i * arcAngle - Math.PI / 2;
            const end = start + arcAngle;

            ctCtx.beginPath();
            ctCtx.moveTo(0, 0);
            ctCtx.arc(0, 0, r, start, end);
            ctCtx.closePath();
            ctCtx.fillStyle = ctColors[i % ctColors.length];
            ctCtx.fill();
            ctCtx.strokeStyle = 'rgba(0,0,0,0.3)';
            ctCtx.lineWidth = 1;
            ctCtx.stroke();

            ctCtx.save();
            const textA = start + arcAngle / 2;
            ctCtx.rotate(textA);
            ctCtx.translate(r * 0.65, 0);
            ctCtx.rotate(Math.PI / 2);
            ctCtx.fillStyle = 'white';
            ctCtx.font = 'bold 14px Outfit';
            ctCtx.textAlign = 'center';
            ctCtx.fillText(`x${segments[i]}`, 0, 0);
            ctCtx.restore();
        }

        // Center
        ctCtx.beginPath();
        ctCtx.arc(0, 0, 24, 0, Math.PI * 2);
        ctCtx.fillStyle = '#1a1a2e';
        ctCtx.fill();
        ctCtx.strokeStyle = 'rgba(255,255,255,0.15)';
        ctCtx.lineWidth = 2;
        ctCtx.stroke();

        ctCtx.restore();
    }

    // Spin animation
    drawCTWheel(0);

    setTimeout(() => {
        const targetAngle = -(winnerIndex * arcAngle + arcAngle / 2);
        const totalAngle = 4 * 2 * Math.PI + targetAngle;
        const startTime = performance.now();
        const spinDuration = 8000;
        let ctAngle = 0;

        function animateCT(now) {
            const elapsed = now - startTime;
            const progress = Math.min(elapsed / spinDuration, 1);
            const eased = 1 - Math.pow(1 - progress, 4);
            ctAngle = totalAngle * eased;
            drawCTWheel(ctAngle);

            if (progress < 1) {
                requestAnimationFrame(animateCT);
            } else {
                // Show result
                const resultEl = document.getElementById('ct-result');
                if (boost !== 'none') {
                    resultEl.innerHTML = `x${baseMult} → <span class="crazytime-boost">${boost.toUpperCase()}!</span> → x${multiplier}`;
                } else {
                    resultEl.textContent = `x${multiplier}`;
                }
                showMinigameResultMultiplier(multiplier);
                setTimeout(onComplete, 4000);
            }
        }

        requestAnimationFrame(animateCT);
    }, 1500);
}

// ===== BALANCE =====
function fetchBalance() {
    if (!currentUser) return;
    fetch(`/api/wallet/balance?username=${encodeURIComponent(currentUser)}`)
        .then(r => r.json())
        .then(d => { if (d.success) updateBalanceDisplay(d.balance); })
        .catch(() => {});
}

// ===== BETTING =====
betButtons.forEach(btn => {
    btn.addEventListener('click', async () => {
        const segment = btn.getAttribute('data-segment');
        const amount = betAmountInput.value;

        if (!amount || parseFloat(amount) <= 0) return;

        try {
            const res = await fetch(`/api/wallet/place-bet?username=${encodeURIComponent(currentUser)}&amount=${amount}&segment=${encodeURIComponent(segment)}`, { method: 'POST' });
            const data = await res.json();

            if (data.success) {
                updateBalanceDisplay(data.new_balance);
                if (!myBetsThisRound[segment]) myBetsThisRound[segment] = 0;
                myBetsThisRound[segment] += parseFloat(amount);
                addChipToButton(btn, myBetsThisRound[segment]);
            } else {
                showBetError(data.error);
            }
        } catch (e) {
            console.error(e);
        }
    });
});

// Amount buttons
document.querySelectorAll('.amount-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const action = btn.getAttribute('data-action');
        let val = parseFloat(betAmountInput.value) || 10;
        if (action === 'half') val = Math.max(1, Math.floor(val / 2));
        if (action === 'double') val = Math.min(val * 2, currentBalance);
        betAmountInput.value = val;
    });
});

function addChipToButton(btn, totalAmount) {
    let chip = btn.querySelector('.bet-chip');
    if (!chip) {
        chip = document.createElement('div');
        chip.className = 'bet-chip';
        btn.appendChild(chip);
    }
    chip.textContent = `$${totalAmount}`;
}

function clearChips() {
    document.querySelectorAll('.bet-chip').forEach(c => c.remove());
}

function showBetError(msg) {
    // Brief visual feedback
    const el = document.createElement('div');
    el.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:rgba(239,68,68,0.9);color:white;padding:10px 24px;border-radius:8px;font-weight:600;z-index:9999;animation:fadeIn 0.3s ease;';
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 2500);
}

// ===== BET ALL =====
betAllBtn.addEventListener('click', async () => {
    if (betAllBtn.disabled) return;
    const amount = betAmountInput.value;
    if (!amount || parseFloat(amount) <= 0) return;
    
    const allSegments = ["1", "2", "5", "10", "Pachinko", "CoinFlip", "CashHunt", "CrazyTime"];
    
    // Disable temporarily
    betAllBtn.disabled = true;
    
    for (let seg of allSegments) {
        const btn = document.querySelector(`.bet-btn[data-segment="${seg}"]`);
        if (btn && !btn.disabled) {
            btn.click();
            await new Promise(r => setTimeout(r, 150)); // stagger requests
        }
    }
    
    betAllBtn.disabled = false;
});

// Close result overlay on click
resultOverlay.addEventListener('click', () => {
    resultOverlay.style.display = 'none';
});
