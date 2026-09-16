import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Separate from vite.config.ts so the app build carries no test configuration.
export default defineConfig({
  plugins: [react()],
  test: {
    // Node by default: most of the suite is pure logic and reads the repo's own
    // sample files. The files that need a DOM ask for one with a
    // `@vitest-environment jsdom` pragma of their own.
    environment: "node",
  },
});
