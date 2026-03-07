import {
  BASE_TICK_MS,
  BOARD_SIZE,
  CELL_SIZE,
  ENTITY_META,
  createInitialState,
  queueDirection,
  restartState,
  stepGame,
  togglePause,
} from "./snake-core.js";

const canvas = document.getElementById("gameBoard");
const ctx = canvas.getContext("2d");
const canvasWrap = document.getElementById("canvasWrap");
const salaryValue = document.getElementById("salaryValue");
const xpValue = document.getElementById("xpValue");
const burnoutValue = document.getElementById("burnoutValue");
const bestValue = document.getElementById("bestValue");
const statusText = document.getElementById("statusText");
const metaText = document.getElementById("metaText");
const pauseButton = document.getElementById("pauseButton");
const restartButton = document.getElementById("restartButton");
const overlay = document.getElementById("overlay");
const overlayEyebrow = document.getElementById("overlayEyebrow");
const overlayTitle = document.getElementById("overlayTitle");
const overlayText = document.getElementById("overlayText");
const overlayAction = document.getElementById("overlayAction");
const directionButtons = [...document.querySelectorAll("[data-dir]")];

const BEST_SCORE_KEY = "job-snake-best-salary";
const randomFn = () => Math.random();

let state = createInitialState(randomFn);
let bestSalary = loadBestSalary();
let timerId = 0;
let touchStart = null;

window.__jobSnakeDebug = {
  getState: () => cloneState(state),
};

initTelegramShell();
attachEvents();
render();
scheduleNextTick();

function initTelegramShell() {
  const tg = window.Telegram?.WebApp;
  if (!tg) {
    return;
  }

  tg.ready();
  tg.expand();

  const theme = tg.themeParams || {};
  if (theme.bg_color) {
    document.documentElement.style.setProperty("--bg0", theme.bg_color);
  }
  if (theme.secondary_bg_color) {
    document.documentElement.style.setProperty("--panel", theme.secondary_bg_color);
  }
  if (theme.text_color) {
    document.documentElement.style.setProperty("--ink", theme.text_color);
  }
  if (theme.hint_color) {
    document.documentElement.style.setProperty("--muted", theme.hint_color);
  }
  if (theme.button_color) {
    document.documentElement.style.setProperty("--blue", theme.button_color);
  }
}

function attachEvents() {
  window.addEventListener("keydown", handleKeyDown);
  pauseButton.addEventListener("click", handlePauseToggle);
  restartButton.addEventListener("click", handleRestart);
  overlayAction.addEventListener("click", handleOverlayAction);

  for (const button of directionButtons) {
    button.addEventListener("click", () => queueInput(button.dataset.dir));
  }

  canvasWrap.addEventListener(
    "touchstart",
    (event) => {
      const touch = event.changedTouches[0];
      touchStart = { x: touch.clientX, y: touch.clientY };
    },
    { passive: true }
  );

  canvasWrap.addEventListener(
    "touchend",
    (event) => {
      if (!touchStart) {
        return;
      }
      const touch = event.changedTouches[0];
      const dx = touch.clientX - touchStart.x;
      const dy = touch.clientY - touchStart.y;
      touchStart = null;

      if (Math.max(Math.abs(dx), Math.abs(dy)) < 28) {
        return;
      }

      if (Math.abs(dx) > Math.abs(dy)) {
        queueInput(dx > 0 ? "right" : "left");
      } else {
        queueInput(dy > 0 ? "down" : "up");
      }
    },
    { passive: true }
  );
}

function handleKeyDown(event) {
  const key = event.key.toLowerCase();

  if (key === "arrowup" || key === "w") {
    event.preventDefault();
    queueInput("up");
    return;
  }
  if (key === "arrowdown" || key === "s") {
    event.preventDefault();
    queueInput("down");
    return;
  }
  if (key === "arrowleft" || key === "a") {
    event.preventDefault();
    queueInput("left");
    return;
  }
  if (key === "arrowright" || key === "d") {
    event.preventDefault();
    queueInput("right");
    return;
  }
  if (key === " " || key === "p") {
    event.preventDefault();
    handlePauseToggle();
    return;
  }
  if (key === "r" || key === "enter") {
    event.preventDefault();
    handleRestart();
  }
}

function handlePauseToggle() {
  state = togglePause(state);
  render();
  scheduleNextTick();
}

function handleRestart() {
  commitBestSalary();
  state = restartState(randomFn);
  render();
  scheduleNextTick();
}

function handleOverlayAction() {
  if (state.status === "gameover") {
    handleRestart();
    return;
  }
  handlePauseToggle();
}

function queueInput(direction) {
  state = queueDirection(state, direction);
  if (state.status === "ready" || state.status === "paused") {
    state = togglePause(state);
  }
  render();
  scheduleNextTick();
}

function scheduleNextTick() {
  window.clearTimeout(timerId);
  if (state.status !== "running") {
    return;
  }

  timerId = window.setTimeout(() => {
    state = stepGame(state, randomFn);
    if (state.status === "gameover") {
      commitBestSalary();
    }
    render();
    scheduleNextTick();
  }, Math.max(45, state.tickMs || BASE_TICK_MS));
}

function loadBestSalary() {
  const raw = window.localStorage.getItem(BEST_SCORE_KEY);
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return 100;
  }
  return Math.max(100, Math.floor(value));
}

function commitBestSalary() {
  if (state.salary > bestSalary) {
    bestSalary = state.salary;
    window.localStorage.setItem(BEST_SCORE_KEY, String(bestSalary));
  }
}

function render() {
  salaryValue.textContent = formatSalary(state.salary);
  xpValue.textContent = String(state.experience);
  burnoutValue.textContent = `${Math.round(state.burnout)}%`;
  bestValue.textContent = formatSalary(bestSalary);
  statusText.textContent = state.message;

  const paceLabel = `Pace +${state.speedLevel}`;
  const bonusLabel = state.skillBoostJobs > 0 ? `Bonus x2 (${state.skillBoostJobs})` : "Bonus x1";
  metaText.textContent = `${paceLabel} | ${bonusLabel}`;
  pauseButton.textContent = state.status === "ready" ? "Start" : state.status === "paused" ? "Resume" : "Pause";

  renderOverlay();
  renderBoard();
}

function renderOverlay() {
  if (state.status === "gameover") {
    overlay.hidden = false;
    overlayEyebrow.textContent = "Run Over";
    overlayTitle.textContent = "You survived the job market.";
    overlayText.textContent = [
      `Salary: ${formatSalary(state.salary)}`,
      `Experience: ${state.experience}`,
      `Burnout: ${Math.round(state.burnout)}%`,
      state.reason,
    ].join(" ");
    overlayAction.textContent = "Restart";
    return;
  }

  if (state.status === "paused") {
    overlay.hidden = false;
    overlayEyebrow.textContent = "Paused";
    overlayTitle.textContent = "Catch your breath.";
    overlayText.textContent = "Resume when you are ready to chase the next offer.";
    overlayAction.textContent = "Resume";
    return;
  }

  if (state.status === "ready") {
    overlay.hidden = false;
    overlayEyebrow.textContent = "Ready";
    overlayTitle.textContent = "Open the hunt.";
    overlayText.textContent = "Start with the button, swipe on the board, or press an arrow key.";
    overlayAction.textContent = "Start";
    return;
  }

  overlay.hidden = true;
}

function renderBoard() {
  ctx.clearRect(0, 0, BOARD_SIZE, BOARD_SIZE);
  ctx.fillStyle = "#08101d";
  ctx.fillRect(0, 0, BOARD_SIZE, BOARD_SIZE);

  drawGrid();

  if (state.job) {
    drawEntity(state.job);
  }
  if (state.special) {
    drawEntity(state.special);
  }

  drawSnake();
}

function drawGrid() {
  ctx.strokeStyle = "rgba(32, 50, 74, 0.7)";
  ctx.lineWidth = 1;

  for (let offset = 0; offset <= BOARD_SIZE; offset += CELL_SIZE) {
    ctx.beginPath();
    ctx.moveTo(offset + 0.5, 0);
    ctx.lineTo(offset + 0.5, BOARD_SIZE);
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(0, offset + 0.5);
    ctx.lineTo(BOARD_SIZE, offset + 0.5);
    ctx.stroke();
  }
}

function drawEntity(entity) {
  const meta = ENTITY_META[entity.type];
  const x = entity.x * CELL_SIZE;
  const y = entity.y * CELL_SIZE;

  ctx.fillStyle = `${meta.tint}22`;
  ctx.fillRect(x + 2, y + 2, CELL_SIZE - 4, CELL_SIZE - 4);

  ctx.font = '18px "Segoe UI Emoji", "Apple Color Emoji", sans-serif';
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(meta.emoji, x + CELL_SIZE / 2, y + CELL_SIZE / 2 + 1);
}

function drawSnake() {
  for (let index = state.snake.length - 1; index >= 0; index -= 1) {
    const segment = state.snake[index];
    const x = segment.x * CELL_SIZE;
    const y = segment.y * CELL_SIZE;

    if (index === 0) {
      ctx.fillStyle = "#78f0ff";
      ctx.fillRect(x + 2, y + 2, CELL_SIZE - 4, CELL_SIZE - 4);
      ctx.font = '18px "Segoe UI Emoji", "Apple Color Emoji", sans-serif';
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("🐍", x + CELL_SIZE / 2, y + CELL_SIZE / 2 + 1);
    } else {
      ctx.fillStyle = index % 2 === 0 ? "#4ea3ff" : "#62d8f5";
      ctx.fillRect(x + 4, y + 4, CELL_SIZE - 8, CELL_SIZE - 8);
    }
  }
}

function formatSalary(amount) {
  return `$${Math.max(0, Math.floor(amount))}`;
}

function cloneState(value) {
  if (typeof window.structuredClone === "function") {
    return window.structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
}
