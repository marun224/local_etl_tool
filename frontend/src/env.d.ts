/// <reference types="vite/client" />

// Vite turns a CSS import into a side effect that injects the stylesheet.
// TypeScript needs telling that such an import is legal and yields nothing.
declare module "*.css";
