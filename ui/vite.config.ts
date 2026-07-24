import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  // Baked-in build id so the running bundle is identifiable at runtime (console
  // banner + /__debug). Helps confirm a redeploy actually replaced the image.
  define: {
    __BUILD_ID__: JSON.stringify(new Date().toISOString()),
  },
  server: {
    port: 5173,
  },
});
