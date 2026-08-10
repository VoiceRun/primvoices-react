import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import { resolve } from "path";
import dts from "vite-plugin-dts";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react(), 
    dts({         
      insertTypesEntry: true,
    }),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, "./src/index.ts"),
      name: "primvoices-react",
      fileName: (format) => `index.${format}.js`,
    },
    rollupOptions: {
      // react/jsx-runtime must be external: bundling it freezes the React
      // version the library was BUILT with (React 19 → elements tagged
      // react.transitional.element), which a React 18 host renderer rejects
      // with minified error #31. Externalized, the host app's own runtime
      // creates the elements. Mirrors voicerun-react 5a3526e.
      external: ["react", "react-dom", "react/jsx-runtime", "react/jsx-dev-runtime", "tailwindcss"],
      output: {
        globals: {
          react: "React",
          "react-dom": "ReactDOM",
          "react/jsx-runtime": "jsxRuntime",
          "react/jsx-dev-runtime": "jsxDevRuntime",
          tailwindcss: "tailwindcss",
        },
      },
    },
    sourcemap: true,
    emptyOutDir: true,
  },
});
