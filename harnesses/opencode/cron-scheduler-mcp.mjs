#!/usr/bin/env node
/**
 * Cron Scheduler MCP server — schedules and runs tasks on device.
 *
 * Tools:
 *   schedule_task  — schedule a recurring or one-shot task
 *   list_tasks     — list all scheduled tasks with status
 *   cancel_task    — cancel and remove a task by ID
 *   run_task_now   — run a task immediately (outside its schedule)
 *
 * Cron expression format (5 space-separated fields):
 *   minute hour day_of_month month day_of_week
 *   Supports: * (wildcard), */n (step), n-m (range), a,b,c (list)
 *   Shortcuts: @hourly @daily @weekly @monthly @yearly @minutely
 *
 * Commands are executed via /bin/sh -c. Output (last 4096 chars) is stored
 * in each task's last_run record.
 *
 * State is persisted to CRON_STORE_PATH (default: /tmp/lap-cron-tasks.json)
 * so tasks survive MCP server restarts.
 *
 * Env vars:
 *   CRON_STORE_PATH       — path for persisted task state (default: /tmp/lap-cron-tasks.json)
 *   CRON_TASK_TIMEOUT_MS  — per-task execution timeout in ms (default: 30000)
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";

const execAsync = promisify(exec);

// ── Config ─────────────────────────────────────────────────────────────────
const STORE_PATH = process.env.CRON_STORE_PATH || "/tmp/lap-cron-tasks.json";
const TASK_TIMEOUT_MS = parseInt(process.env.CRON_TASK_TIMEOUT_MS || "30000", 10);
const MAX_OUTPUT_CHARS = 4096;
const TICK_MS = 60_000; // check every 60 s

// ── Cron expression parsing ────────────────────────────────────────────────
const SHORTCUTS = {
  "@yearly":   "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly":  "0 0 1 * *",
  "@weekly":   "0 0 * * 0",
  "@daily":    "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@hourly":   "0 * * * *",
  "@minutely": "* * * * *",
};

function normalizeCron(expr) {
  return SHORTCUTS[expr.trim().toLowerCase()] ?? expr.trim();
}

function matchField(value, field) {
  if (field === "*") return true;
  if (field.includes("/")) {
    const [range, stepStr] = field.split("/");
    const step = parseInt(stepStr, 10);
    if (isNaN(step) || step <= 0) return false;
    let min = 0, max = 59;
    if (range !== "*") {
      const bounds = range.split("-").map(Number);
      min = bounds[0]; max = bounds[1] ?? bounds[0];
    }
    return value >= min && value <= max && (value - min) % step === 0;
  }
  for (const part of field.split(",")) {
    if (part.includes("-")) {
      const [lo, hi] = part.split("-").map(Number);
      if (value >= lo && value <= hi) return true;
    } else if (parseInt(part, 10) === value) {
      return true;
    }
  }
  return false;
}

function matchesCron(expr, date) {
  const parts = normalizeCron(expr).split(/\s+/);
  if (parts.length !== 5) return false;
  const [mF, hF, domF, monF, dowF] = parts;
  return (
    matchField(date.getMinutes(),     mF)   &&
    matchField(date.getHours(),       hF)   &&
    matchField(date.getDate(),        domF) &&
    matchField(date.getMonth() + 1,   monF) &&
    matchField(date.getDay(),         dowF)
  );
}

function validateCron(expr) {
  const norm = normalizeCron(expr);
  const parts = norm.split(/\s+/);
  if (parts.length !== 5)
    return "must have 5 fields: minute hour day_of_month month day_of_week";
  for (const p of parts) {
    if (!/^[\d*/,\-]+$/.test(p)) return `invalid field: "${p}"`;
  }
  return null; // ok
}

// ── State ──────────────────────────────────────────────────────────────────
/** @type {Map<string, object>} id -> task */
const tasks = new Map();
/** @type {Set<string>} IDs of tasks currently running */
const running = new Set();

function loadTasks() {
  try {
    const arr = JSON.parse(readFileSync(STORE_PATH, "utf-8"));
    for (const t of arr) tasks.set(t.id, t);
    console.error(`[cron-mcp] loaded ${tasks.size} task(s) from ${STORE_PATH}`);
  } catch (e) {
    if (e.code !== "ENOENT")
      console.error(`[cron-mcp] warn: could not load tasks: ${e.message}`);
  }
}

function saveTasks() {
  try {
    mkdirSync(dirname(STORE_PATH), { recursive: true });
    writeFileSync(STORE_PATH, JSON.stringify([...tasks.values()], null, 2));
  } catch (e) {
    console.error(`[cron-mcp] warn: could not save tasks: ${e.message}`);
  }
}

// ── Task execution ─────────────────────────────────────────────────────────
async function executeTask(task) {
  if (running.has(task.id)) {
    console.error(`[cron-mcp] task ${task.id} already running, skipping tick`);
    return;
  }
  running.add(task.id);
  const startedAt = new Date().toISOString();
  task.last_run = { started_at: startedAt, status: "running", output: "" };
  saveTasks();
  try {
    const { stdout, stderr } = await execAsync(task.command, {
      timeout: TASK_TIMEOUT_MS,
      shell: "/bin/sh",
      cwd: task.cwd || process.cwd(),
    });
    const output = (stdout + stderr).slice(-MAX_OUTPUT_CHARS);
    task.last_run = {
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      status: "success",
      output,
    };
    task.run_count = (task.run_count || 0) + 1;
    console.error(`[cron-mcp] task ${task.id} "${task.name}" completed OK`);
  } catch (e) {
    const output = ((e.stdout || "") + (e.stderr || "") + e.message).slice(-MAX_OUTPUT_CHARS);
    task.last_run = {
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      status: "error",
      output,
    };
    console.error(`[cron-mcp] task ${task.id} "${task.name}" failed: ${e.message}`);
  } finally {
    running.delete(task.id);
    saveTasks();
  }
}

// ── Scheduler tick ─────────────────────────────────────────────────────────
let lastTickMinute = -1;

function tick() {
  const now = new Date();
  const minute = now.getMinutes();
  if (minute === lastTickMinute) return; // already checked this minute
  lastTickMinute = minute;
  for (const task of tasks.values()) {
    if (!task.enabled) continue;
    if (matchesCron(task.cron_expression, now)) {
      executeTask(task).catch((e) =>
        console.error(`[cron-mcp] unexpected error running task ${task.id}: ${e.message}`),
      );
    }
  }
}

// ── MCP server ─────────────────────────────────────────────────────────────
const server = new Server(
  { name: "cron-scheduler", version: "1.0.0" },
  { capabilities: { tools: {} } },
);

const TOOLS = [
  {
    name: "schedule_task",
    description:
      "Schedule a task to run on this device on a recurring cron schedule. " +
      "The command is executed as a shell script via /bin/sh -c. " +
      "Returns the unique task ID. Supports standard 5-field cron syntax " +
      "(minute hour day_of_month month day_of_week) and shortcuts like " +
      "@hourly, @daily, @weekly, @monthly.",
    inputSchema: {
      type: "object",
      properties: {
        name: {
          type: "string",
          description: "Human-readable name for the task (e.g. 'daily-backup').",
        },
        cron_expression: {
          type: "string",
          description:
            "Cron schedule. 5-field format: 'minute hour dom month dow'. " +
            "Examples: '0 9 * * 1-5' (weekdays 9 am), '*/15 * * * *' (every 15 min), " +
            "'0 0 * * *' (midnight). Shortcuts: @hourly @daily @weekly @monthly.",
        },
        command: {
          type: "string",
          description: "Shell command to run when the task fires (via /bin/sh -c).",
        },
        description: {
          type: "string",
          description: "Optional description of what the task does.",
        },
        cwd: {
          type: "string",
          description: "Working directory for the command. Defaults to the process cwd.",
        },
        enabled: {
          type: "boolean",
          description: "Whether the task is active immediately. Defaults to true.",
        },
      },
      required: ["name", "cron_expression", "command"],
    },
  },
  {
    name: "list_tasks",
    description:
      "List all scheduled cron tasks, including schedule, last-run status, and run count.",
    inputSchema: {
      type: "object",
      properties: {
        include_disabled: {
          type: "boolean",
          description: "Include disabled tasks. Defaults to true.",
        },
      },
    },
  },
  {
    name: "cancel_task",
    description: "Cancel and permanently remove a scheduled task by its ID.",
    inputSchema: {
      type: "object",
      properties: {
        task_id: {
          type: "string",
          description: "Task ID (from schedule_task or list_tasks).",
        },
      },
      required: ["task_id"],
    },
  },
  {
    name: "run_task_now",
    description:
      "Run a scheduled task immediately, outside its normal cron schedule. " +
      "Waits for the task to complete and returns its output.",
    inputSchema: {
      type: "object",
      properties: {
        task_id: {
          type: "string",
          description: "Task ID to execute now.",
        },
      },
      required: ["task_id"],
    },
  },
];

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

// ── Tool handlers ──────────────────────────────────────────────────────────
function ok(text) { return { content: [{ type: "text", text }], isError: false }; }
function fail(text) { return { content: [{ type: "text", text }], isError: true }; }

function handleScheduleTask({ name, cron_expression, command, description, cwd, enabled }) {
  if (!name || typeof name !== "string" || !name.trim())
    return fail("schedule_task failed: name is required");
  if (!command || typeof command !== "string" || !command.trim())
    return fail("schedule_task failed: command is required");
  const cronError = validateCron(cron_expression ?? "");
  if (cronError) return fail(`schedule_task failed: invalid cron expression — ${cronError}`);

  const id = randomUUID();
  const normalized = normalizeCron(cron_expression);
  const task = {
    id,
    name: name.trim(),
    cron_expression: cron_expression.trim(),
    command: command.trim(),
    description: (description ?? "").trim(),
    cwd: cwd?.trim() || null,
    enabled: enabled !== false,
    created_at: new Date().toISOString(),
    run_count: 0,
    last_run: null,
  };
  tasks.set(id, task);
  saveTasks();
  console.error(`[cron-mcp] scheduled task id=${id} name="${task.name}" cron="${cron_expression}"`);
  return ok(
    `Task scheduled successfully.\n` +
    `ID:       ${id}\n` +
    `Name:     ${task.name}\n` +
    `Schedule: ${cron_expression}${normalized !== cron_expression.trim() ? ` (= ${normalized})` : ""}\n` +
    `Command:  ${task.command}`,
  );
}

function handleListTasks({ include_disabled = true } = {}) {
  const list = [...tasks.values()].filter((t) => include_disabled || t.enabled);
  if (list.length === 0) return ok("No scheduled tasks.");
  const lines = list.map((t) => {
    const lastRun = t.last_run
      ? `${t.last_run.status} at ${t.last_run.finished_at ?? t.last_run.started_at}`
      : "never run";
    const rows = [
      `• [${t.id}] ${t.name}${t.enabled ? "" : " (disabled)"}`,
      `  schedule : ${t.cron_expression}`,
      `  command  : ${t.command}`,
      t.description ? `  desc     : ${t.description}` : null,
      `  runs     : ${t.run_count ?? 0}   last: ${lastRun}`,
    ];
    return rows.filter(Boolean).join("\n");
  });
  return ok(lines.join("\n\n"));
}

function handleCancelTask({ task_id }) {
  const task = tasks.get(task_id);
  if (!task) return fail(`cancel_task failed: no task with ID "${task_id}"`);
  tasks.delete(task_id);
  saveTasks();
  return ok(`Task "${task.name}" (${task_id}) has been cancelled and removed.`);
}

async function handleRunTaskNow({ task_id }) {
  const task = tasks.get(task_id);
  if (!task) return fail(`run_task_now failed: no task with ID "${task_id}"`);
  if (running.has(task_id))
    return fail(`run_task_now failed: task "${task.name}" is already running`);
  console.error(`[cron-mcp] running task now: id=${task_id} name="${task.name}"`);
  await executeTask(task);
  const run = task.last_run;
  if (!run) return fail("run_task_now: no run record after execution (unexpected)");
  const duration = run.finished_at
    ? `${new Date(run.finished_at) - new Date(run.started_at)}ms`
    : "unknown";
  return ok(
    `Task "${task.name}" completed.\n` +
    `Status:   ${run.status}\n` +
    `Duration: ${duration}\n` +
    `Output:\n${run.output || "(no output)"}`,
  );
}

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;
  if (name === "schedule_task") return handleScheduleTask(args ?? {});
  if (name === "list_tasks")    return handleListTasks(args ?? {});
  if (name === "cancel_task")   return handleCancelTask(args ?? {});
  if (name === "run_task_now")  return handleRunTaskNow(args ?? {});
  return fail(`unknown tool: ${name}`);
});

// ── Boot ────────────────────────────────────────────────────────────────────
loadTasks();

// Align first tick to the start of the next minute, then tick every 60 s.
const now = new Date();
const msToNextMinute = (60 - now.getSeconds()) * 1000 - now.getMilliseconds();
setTimeout(() => {
  tick();
  setInterval(tick, TICK_MS);
}, msToNextMinute);

process.on("SIGINT",  () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));

const transport = new StdioServerTransport();
await server.connect(transport);
console.error(
  `[cron-mcp] ready — ${tasks.size} task(s) loaded, ` +
  `store=${STORE_PATH}, timeout=${TASK_TIMEOUT_MS}ms`,
);
