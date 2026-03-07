export const GRID_SIZE = 20;
export const CELL_SIZE = 24;
export const BOARD_SIZE = GRID_SIZE * CELL_SIZE;
export const BASE_TICK_MS = 120;
export const MIN_TICK_MS = 70;

const START_SNAKE = [
  { x: 10, y: 10 },
  { x: 9, y: 10 },
  { x: 8, y: 10 },
];
const MIN_SNAKE_LENGTH = 2;
const BURNOUT_PER_STEP = 0.1;
const SKILL_BOOST_JOBS = 3;
const SPECIAL_SPAWN_CHANCE = 0.18;
const SPECIAL_WEIGHTS = [
  { type: "skill", weight: 10 },
  { type: "ai", weight: 5 },
  { type: "ghost", weight: 10 },
  { type: "layoff", weight: 5 },
];

export const ENTITY_META = {
  job: { emoji: "💼", label: "Job", tint: "#4ea3ff" },
  skill: { emoji: "🧠", label: "Skill", tint: "#78f0ff" },
  ai: { emoji: "🤖", label: "AI", tint: "#ffcc7a" },
  ghost: { emoji: "👻", label: "Ghost Job", tint: "#ff5f70" },
  layoff: { emoji: "📉", label: "Layoff", tint: "#ff8b6b" },
};

const DIRECTION_VECTORS = {
  up: { x: 0, y: -1 },
  down: { x: 0, y: 1 },
  left: { x: -1, y: 0 },
  right: { x: 1, y: 0 },
};

const OPPOSITES = {
  up: "down",
  down: "up",
  left: "right",
  right: "left",
};

function cloneCell(cell) {
  return { x: cell.x, y: cell.y };
}

function sameCell(a, b) {
  return !!a && !!b && a.x === b.x && a.y === b.y;
}

function cellKey(cell) {
  return `${cell.x}:${cell.y}`;
}

function roll(randomFn) {
  const value = Number(randomFn());
  if (!Number.isFinite(value)) {
    return Math.random();
  }
  if (value <= 0) {
    return 0;
  }
  if (value >= 1) {
    return 0.999999;
  }
  return value;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function wrapCoordinate(value) {
  if (value < 0) {
    return GRID_SIZE - 1;
  }
  if (value >= GRID_SIZE) {
    return 0;
  }
  return value;
}

function createBaseState() {
  return {
    snake: START_SNAKE.map(cloneCell),
    direction: "right",
    queuedDirection: "right",
    job: null,
    special: null,
    salary: 100,
    experience: 0,
    burnout: 0,
    skillBoostJobs: 0,
    speedLevel: 0,
    tickMs: BASE_TICK_MS,
    status: "ready",
    reason: "",
    turn: 0,
    message: "Tap Start, swipe, or press an arrow key to begin.",
  };
}

function buildBlockedSet(state, { includeJob = true, includeSpecial = true } = {}) {
  const blocked = new Set();

  for (const segment of state.snake) {
    blocked.add(cellKey(segment));
  }
  if (includeJob && state.job) {
    blocked.add(cellKey(state.job));
  }
  if (includeSpecial && state.special) {
    blocked.add(cellKey(state.special));
  }

  return blocked;
}

function pickEmptyCell(
  state,
  randomFn,
  { includeJob = true, includeSpecial = true } = {}
) {
  const blocked = buildBlockedSet(state, { includeJob, includeSpecial });
  const openCells = [];

  for (let y = 0; y < GRID_SIZE; y += 1) {
    for (let x = 0; x < GRID_SIZE; x += 1) {
      const key = `${x}:${y}`;
      if (!blocked.has(key)) {
        openCells.push({ x, y });
      }
    }
  }

  if (!openCells.length) {
    return null;
  }

  const index = Math.floor(roll(randomFn) * openCells.length);
  return openCells[index] ?? openCells[0];
}

function pickWeightedSpecialType(randomFn) {
  const totalWeight = SPECIAL_WEIGHTS.reduce((sum, entry) => sum + entry.weight, 0);
  let cursor = roll(randomFn) * totalWeight;

  for (const entry of SPECIAL_WEIGHTS) {
    cursor -= entry.weight;
    if (cursor <= 0) {
      return entry.type;
    }
  }

  return SPECIAL_WEIGHTS[SPECIAL_WEIGHTS.length - 1].type;
}

function spawnJob(state, randomFn) {
  const cell = pickEmptyCell(state, randomFn, { includeJob: false, includeSpecial: true });
  return cell ? { type: "job", ...cell } : null;
}

function spawnSpecial(state, randomFn) {
  if (state.special || roll(randomFn) > SPECIAL_SPAWN_CHANCE) {
    return null;
  }

  const cell = pickEmptyCell(state, randomFn, { includeJob: true, includeSpecial: false });
  if (!cell) {
    return null;
  }

  return {
    type: pickWeightedSpecialType(randomFn),
    ...cell,
  };
}

function trimSnake(snake, desiredLength) {
  return snake.slice(0, Math.max(MIN_SNAKE_LENGTH, desiredLength));
}

function finishGame(state, reason) {
  return {
    ...state,
    status: "gameover",
    reason,
    message: reason,
    queuedDirection: state.direction,
    burnout: clamp(state.burnout, 0, 100),
  };
}

export function createInitialState(randomFn = Math.random) {
  const state = createBaseState();
  state.job = spawnJob(state, randomFn);
  state.special = spawnSpecial(state, randomFn);
  return state;
}

export function restartState(randomFn = Math.random) {
  return createInitialState(randomFn);
}

export function queueDirection(state, nextDirection) {
  if (!DIRECTION_VECTORS[nextDirection]) {
    return state;
  }

  const currentDirection = state.queuedDirection || state.direction;
  if (OPPOSITES[currentDirection] === nextDirection) {
    return state;
  }

  return {
    ...state,
    queuedDirection: nextDirection,
  };
}

export function togglePause(state) {
  if (state.status === "gameover") {
    return state;
  }

  if (state.status === "ready") {
    return {
      ...state,
      status: "running",
      message: "The hunt is live. Wrap through the edges.",
    };
  }

  if (state.status === "paused") {
    return {
      ...state,
      status: "running",
      message: "Back in the hunt.",
    };
  }

  return {
    ...state,
    status: "paused",
    message: "Paused. The market will still be bad in five seconds.",
  };
}

export function stepGame(state, randomFn = Math.random) {
  if (state.status !== "running") {
    return state;
  }

  const direction = state.queuedDirection || state.direction;
  const vector = DIRECTION_VECTORS[direction];
  const currentHead = state.snake[0];
  const nextHead = {
    x: wrapCoordinate(currentHead.x + vector.x),
    y: wrapCoordinate(currentHead.y + vector.y),
  };

  const ateJob = sameCell(nextHead, state.job);
  const bodyToCheck = state.snake.slice(0, ateJob ? state.snake.length : state.snake.length - 1);
  if (bodyToCheck.some((segment) => sameCell(segment, nextHead))) {
    return finishGame(
      {
        ...state,
        direction,
        queuedDirection: direction,
      },
      "You folded into your own pipeline."
    );
  }

  let snake = [nextHead, ...state.snake.map(cloneCell)];
  if (!ateJob) {
    snake.pop();
  }

  let salary = state.salary;
  let experience = state.experience;
  let burnout = clamp(state.burnout + BURNOUT_PER_STEP, 0, 100);
  let skillBoostJobs = state.skillBoostJobs;
  let speedLevel = state.speedLevel;
  let tickMs = state.tickMs;
  let job = state.job ? { ...state.job } : null;
  let special = state.special ? { ...state.special } : null;
  let message = "Keep moving. The walls loop.";

  if (ateJob) {
    const salaryGain = skillBoostJobs > 0 ? 20 : 10;
    salary += salaryGain;
    experience += 1;
    burnout = clamp(burnout + 1, 0, 100);
    if (skillBoostJobs > 0) {
      skillBoostJobs -= 1;
    }
    job = null;
    message = `Offer secured: +$${salaryGain}.`;
  }

  if (special && sameCell(nextHead, special)) {
    if (special.type === "skill") {
      skillBoostJobs = SKILL_BOOST_JOBS;
      experience += 1;
      burnout = clamp(burnout + 1, 0, 100);
      message = "Skill spike: the next 3 jobs pay double.";
    } else if (special.type === "ai") {
      speedLevel += 1;
      tickMs = Math.max(MIN_TICK_MS, BASE_TICK_MS - speedLevel * 10);
      burnout = clamp(burnout + 3, 0, 100);
      message = "AI boost: faster loop, higher pressure.";
    } else if (special.type === "ghost") {
      salary = Math.max(0, salary - 10);
      burnout = clamp(burnout + 5, 0, 100);
      snake = trimSnake(snake, snake.length - 1);
      message = "Ghost job: -$10 and one segment gone.";
    } else if (special.type === "layoff") {
      salary = Math.max(0, salary - 30);
      burnout = clamp(burnout + 10, 0, 100);
      snake = trimSnake(snake, Math.ceil(snake.length / 2));
      message = "Layoff event: half the pipeline is gone.";
    }
    special = null;
  }

  let nextState = {
    ...state,
    snake,
    direction,
    queuedDirection: direction,
    job,
    special,
    salary,
    experience,
    burnout,
    skillBoostJobs,
    speedLevel,
    tickMs,
    turn: state.turn + 1,
    message,
  };

  if (!nextState.job) {
    nextState = {
      ...nextState,
      job: spawnJob(nextState, randomFn),
    };
  }

  if (!nextState.special) {
    nextState = {
      ...nextState,
      special: spawnSpecial(nextState, randomFn),
    };
  }

  return nextState;
}
