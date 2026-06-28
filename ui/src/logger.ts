type LogLevel = "debug" | "info" | "warn" | "error";

const STORAGE_KEY = "deep-research-ui:logging";

function enabled(): boolean {
  const value = localStorage.getItem(STORAGE_KEY);
  return value == null || value === "true";
}

function write(level: LogLevel, step: string, data?: Record<string, unknown>): void {
  if (!enabled()) {
    return;
  }
  const payload = {
    at: new Date().toISOString(),
    step,
    ...(data ?? {}),
  };
  console[level]("[deep-research-ui]", payload);
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
};
