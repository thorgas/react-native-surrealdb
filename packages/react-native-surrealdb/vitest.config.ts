import { defineConfig } from "vitest/config";

// Keep worktree/nested checkouts from inheriting a containing application's suite.
export default defineConfig({ test: { include: ["test/**/*.test.ts"] } });
