import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { SimpleAgentTest } from "./SimpleAgentTest";
import { ErrorBoundary } from "./ErrorBoundary";
import "./styles.css";

const Root = new URLSearchParams(window.location.search).has("simple") ? SimpleAgentTest : App;

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <Root />
    </ErrorBoundary>
  </React.StrictMode>,
);
