import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "lib/index.ts",
  },
  format: ["cjs", "esm"],
  outDir: "out",
  sourcemap: true,
  dts: {
    sourcemap: true,
  },
  exports: true,
});
