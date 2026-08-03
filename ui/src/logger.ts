type LogLevel = "debug" | "info" | "warn" | "error";
export type LogMode = "stream" | "tokens" | "off" | "error" | "warn" | "info" | "debug";

const STORAGE_KEY = "deep-agent-ga-ui:logging";
const MODE_STORAGE_KEY = "deep-agent-ga-ui:log-mode";
export const LOG_MODES: LogMode[] = ["stream", "tokens", "off", "error", "warn", "info", "debug"];
const LEVEL_RANK: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function legacyEnabled(): boolean {
  const value = localStorage.getItem(STORAGE_KEY);
  return value == null || value === "true";
}

export function getLogMode(): LogMode {
  const value = localStorage.getItem(MODE_STORAGE_KEY);
  if (LOG_MODES.includes(value as LogMode)) {
    return value as LogMode;
  }
  if (!legacyEnabled()) {
    return "off";
  }
  return "stream";
}

export function setLogMode(mode: LogMode): void {
  localStorage.setItem(MODE_STORAGE_KEY, mode);
}

function enabled(level: LogLevel): boolean {
  const mode = getLogMode();
  if (mode === "off" || mode === "tokens" || mode === "stream") {
    return false;
  }
  return LEVEL_RANK[level] >= LEVEL_RANK[mode];
}

function tokensEnabled(): boolean {
  const mode = getLogMode();
  return mode === "stream" || mode === "tokens" || mode === "info" || mode === "debug";
}

function streamProbeEnabled(): boolean {
  const mode = getLogMode();
  return mode === "stream" || mode === "info" || mode === "debug";
}

function write(level: LogLevel, step: string, data?: Record<string, unknown>, force = false): void {
  if (!force && !enabled(level)) {
    return;
  }
  const payload = {
    at: new Date().toISOString(),
    step,
    ...(data ?? {}),
  };
  console[level]("[deep-agent-ga-ui]", payload);
}

export const logger = {
  debug(step: string, data?: Record<string, unknown>): void {
    write("debug", step, data);
  },
  info(step: string, data?: Record<string, unknown>): void {
    write("info", step, data);
  },
  warn(step: string, data?: Record<string, unknown>): void {
    write("warn", step, data);
  },
  error(step: string, data?: Record<string, unknown>): void {
    write("error", step, data);
  },
  token(step: string, data?: Record<string, unknown>): void {
    if (!tokensEnabled()) {
      return;
    }
    write("info", step, data, true);
  },
  stream(step: string, data?: Record<string, unknown>): void {
    if (!streamProbeEnabled()) {
      return;
    }
    write("info", step, data, true);
  },
};
