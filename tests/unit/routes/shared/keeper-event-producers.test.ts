import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import * as ts from "typescript";

// Guard every event constructor, including failures and direct fallback requests.
describe("Keeper event producer contract", () => {
  it.each([
    ["streaming-handler", 2],
    ["non-streaming-handler", 2],
    ["direct-request-handler", 3],
  ] as const)("%s preserves IDs and reasoning effort on every terminal path", (name, expectedCount) => {
    const path = `src/routes/shared/${name}.ts`;
    const file = ts.createSourceFile(path, readFileSync(path, "utf8"), ts.ScriptTarget.Latest, true);
    let count = 0;
    const visit = (node: ts.Node): void => {
      if (ts.isObjectLiteralExpression(node)) {
        const fields = new Map(node.properties.filter(ts.isPropertyAssignment)
          .map((property) => [property.name.getText(file), property.initializer.getText(file)]));
        if (fields.get("schema") === "KEEPER_EVENT_SCHEMA") {
          count++;
          const transport = name === "streaming-handler" || (name === "direct-request-handler" && count === 1) ? "sse" : "http";
          expect(fields.get("downstream_transport")).toBe(JSON.stringify(transport));
          expect(fields.get("request_id")).toBe("requestId");
          for (const key of ["event_id", "attempt_id"]) {
            expect(fields.get(key)).toMatch(/requestId|attemptId/);
          }
          expect(fields.get("reasoning_effort")).toBe("req.codexRequest.reasoning?.effort ?? null");
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(file);
    expect(count).toBe(expectedCount);
  });
});
