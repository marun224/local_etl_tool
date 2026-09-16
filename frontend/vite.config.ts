import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Tauri serves the dev build from a fixed port and loads the production build
// from disk, so asset URLs must be relative rather than rooted at "/".
export default defineConfig({
  plugins: [react()],
  base: "./",
  server: {
    port: 5173,
    strictPort: true,
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Tauri ships its own webview, so there is no older browser to support.
    target: "esnext",
  },
});
