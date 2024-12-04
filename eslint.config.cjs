// @ts-check

const foxglove = require("@foxglove/eslint-plugin");
const tseslint = require("typescript-eslint");

module.exports = tseslint.config(
  {
    ignores: ["**/dist"],
  },
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.json",
      },
    },
    rules: {
      "@typescript-eslint/explicit-member-accessibility": "error",
    },
  },
  ...foxglove.configs.base,
  ...foxglove.configs.jest,
  ...foxglove.configs.typescript.map((config) => ({
    files: ["**/*.ts", "**/*.tsx"],
    ...config,
  })),
);
