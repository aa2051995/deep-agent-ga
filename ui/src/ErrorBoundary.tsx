import { Component } from "react";
import type { ErrorInfo, ReactNode } from "react";
import { logger } from "./logger";

type Props = {
  children: ReactNode;
};

type State = {
  error: Error | null;
};

/**
 * Catches render/hook errors — notably throws from inside the LangGraph SDK's
 * `useStream` (e.g. "Unexpected tool event: undefined") — so one bad stream
 * event degrades to a recoverable message instead of blanking the whole app.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    logger.error("ui.error_boundary.caught", {
      message: error.message,
      stack: error.stack,
      componentStack: info.componentStack,
    });
  }

  private handleReload = (): void => {
    this.setState({ error: null });
    window.location.reload();
  };

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="app-error-boundary" role="alert">
          <h1>Something went wrong</h1>
          <p>The interface hit an unexpected error while streaming. Your research is safe — it is saved on the server.</p>
          <pre className="app-error-boundary-detail">{this.state.error.message}</pre>
          <button type="button" className="app-error-boundary-button" onClick={this.handleReload}>
            Reload
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
