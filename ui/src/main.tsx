import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { SimpleAgentTest } from "./SimpleAgentTest";
import { AssistantManager } from "./AssistantManager";
import { ErrorBoundary } from "./ErrorBoundary";
import "./styles.css";

const params = new URLSearchParams(window.location.search);
const Root = params.has("assistants")
  ? AssistantManager
  : params.has("simple")
    ? SimpleAgentTest
    : App;

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <Root />
    </ErrorBoundary>
  </React.StrictMode>,
);
