/** Narrow redaction for JSON payloads and embedded SSE/debug chunks; never mutates the wire. */
export function redactTurnStateJson(value: unknown): string | undefined {
  return JSON.stringify(value, (key, item: unknown) => {
    if (/^(?:turnState|x-codex-turn-state)$/i.test(key)) return "[redacted]";
    if (typeof item === "string") {
      return item.replace(/((?:\\?"?)(?:x-codex-turn-state|turnState)(?:\\?"?)\s*:\s*(?:\\?"))[^"\\\r\n]*/gi, "$1[redacted]");
    }
    return item;
  });
}
