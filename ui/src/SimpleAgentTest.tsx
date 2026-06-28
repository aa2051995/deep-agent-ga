import { useCallback, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { Loader2, MessageSquare, Plus, Send, Square } from "lucide-react";
import { useStream } from "@langchain/langgraph-sdk/react";
import type { Message } from "@langchain/langgraph-sdk";
import type { BaseMessage } from "@langchain/core/messages";
import type { AgentState } from "./simpleTypes";
import { DEFAULT_API_URL } from "./stream";

const SIMPLE_THREAD_KEY = "deep-research-ui:simple-thread";
const SIMPLE_THREADS_KEY = "deep-research-ui:simple-threads";
const ASSISTANT_ID = "deep-agent";
const STREAM_MODES = ["messages-tuple", "values"] as const;
const PRESETS = [
  "Compare React, Vue, and Svelte for a dashboard",
  "Give a short brief on AI infrastructure trends",
  "Research the best approach for autoscaling Kubernetes workloads",
];

type RenderMessage = BaseMessage | Message;
type SimpleThread = {
  threadId: string;
  title: string;
  updatedAt: string;
};

function loadThreads(): SimpleThread[] {
  try {
    const raw = sessionStorage.getItem(SIMPLE_THREADS_KEY);
    return raw ? (JSON.parse(raw) as SimpleThread[]) : [];
  } catch {
    return [];
  }
}

function saveThreads(threads: SimpleThread[]): void {
  sessionStorage.setItem(SIMPLE_THREADS_KEY, JSON.stringify(threads));
}

function upsertThread(threads: SimpleThread[], threadId: string, title: string): SimpleThread[] {
  const updatedAt = new Date().toISOString();
  const existing = threads.find((thread) => thread.threadId === threadId);
  const next = existing
    ? threads.map((thread) =>
        thread.threadId === threadId
          ? { ...thread, title: thread.title === "New chat" ? title : thread.title, updatedAt }
          : thread,
      )
    : [{ threadId, title, updatedAt }, ...threads];
  return next.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt)).slice(0, 20);
}

function messageText(message: RenderMessage): string {
  if (typeof message.content === "string") {
    return message.content;
  }
  if (Array.isArray(message.content)) {
    return message.content
      .map((block) => {
        if (typeof block === "string") {
          return block;
        }
        if (typeof block === "object" && block !== null && "text" in block) {
          return String(block.text ?? "");
        }
        return "";
      })
      .join("");
  }
  return "";
}

function messageRole(message: RenderMessage): string {
  if ("type" in message && typeof message.type === "string") {
    return message.type;
  }
  return "message";
}

export function SimpleAgentTest() {
  const [apiUrl, setApiUrl] = useState(DEFAULT_API_URL);
  const [threadId, setThreadId] = useState<string | null>(() => sessionStorage.getItem(SIMPLE_THREAD_KEY));
  const [threads, setThreads] = useState<SimpleThread[]>(loadThreads);
  const [draft, setDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const pendingTitle = useRef("New chat");

  const updateThreadId = useCallback((id: string | null) => {
    setThreadId(id);
    if (id) {
      sessionStorage.setItem(SIMPLE_THREAD_KEY, id);
    } else {
      sessionStorage.removeItem(SIMPLE_THREAD_KEY);
    }
  }, []);

  const rememberThread = useCallback((id: string, title: string) => {
    setThreads((current) => {
      const next = upsertThread(current, id, title);
      saveThreads(next);
      return next;
    });
  }, []);

  const stream = useStream<AgentState>({
    apiUrl,
    assistantId: ASSISTANT_ID,
    threadId,
    onThreadId: (id) => {
      updateThreadId(id);
      rememberThread(id, pendingTitle.current);
    },
    messagesKey: "messages",
  });

  const messages = useMemo(() => stream.messages ?? [], [stream.messages]);

  async function submitContent(content: string): Promise<void> {
    if (!content || stream.isLoading) {
      return;
    }
    pendingTitle.current = content;
    setDraft("");
    setError(null);
    try {
      await stream.submit(
        { messages: [{ type: "human", content }] } as unknown as Partial<AgentState>,
        {
          streamMode: [...STREAM_MODES],
          streamSubgraphs: true,
          streamResumable: true,
          multitaskStrategy: "reject",
        },
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  async function submit(): Promise<void> {
    await submitContent(draft.trim());
  }

  function submitPreset(content: string): void {
    void submitContent(content);
  }

  function newThread(): void {
    updateThreadId(null);
    stream.switchThread(null);
    setError(null);
  }

  function openThread(id: string): void {
    updateThreadId(id);
    stream.switchThread(id);
    setError(null);
  }

  return (
    <div className="simple-app">
      <aside className="simple-sidebar">
        <button className="simple-new-chat" onClick={newThread} type="button">
          <Plus size={16} />
          New chat
        </button>
        <div className="simple-thread-list">
          {threads.map((thread) => (
            <button
              className={thread.threadId === threadId ? "simple-thread active" : "simple-thread"}
              key={thread.threadId}
              onClick={() => openThread(thread.threadId)}
              title={thread.threadId}
              type="button"
            >
              <MessageSquare size={15} />
              <span>{thread.title}</span>
            </button>
          ))}
        </div>
        <label className="simple-api">
          <span>API URL</span>
          <input value={apiUrl} onChange={(event) => setApiUrl(event.target.value)} />
        </label>
      </aside>

      <ChatContainer
        error={error}
        input={
          <ChatInput
            draft={draft}
            disabled={stream.isLoading}
            onChange={setDraft}
            onStop={() => void stream.stop()}
            onSubmit={() => void submit()}
            loading={stream.isLoading}
          />
        }
      >
        {messages.length === 0 ? (
          <PresetPrompts prompts={PRESETS} onSelect={submitPreset} />
        ) : (
          messages.map((message, index) =>
            messageRole(message) === "human" ? (
              <HumanBubble key={message.id ?? index}>
                <Markdown>{messageText(message)}</Markdown>
              </HumanBubble>
            ) : (
              <AIBubble key={message.id ?? index}>
                <Markdown>{messageText(message) || "..."}</Markdown>
              </AIBubble>
            ),
          )
        )}
        {stream.isLoading && <TypingIndicator />}
      </ChatContainer>
    </div>
  );
}

function ChatContainer({
  children,
  error,
  input,
}: {
  children: ReactNode;
  error: string | null;
  input: ReactNode;
}) {
  return (
    <main className="simple-chat">
      <header className="simple-chat-header">
        <div>
          <h1>Deep Research</h1>
          <span>Simple `useStream&lt;AgentState&gt;` test</span>
        </div>
      </header>
      <section className="simple-messages">{children}</section>
      {error && <div className="simple-error">{error}</div>}
      {input}
    </main>
  );
}

function HumanBubble({ children }: { children: ReactNode }) {
  return <article className="simple-bubble human">{children}</article>;
}

function AIBubble({ children }: { children: ReactNode }) {
  return <article className="simple-bubble ai">{children}</article>;
}

function Markdown({ children }: { children: string }) {
  return <p>{children}</p>;
}

function TypingIndicator() {
  return (
    <div className="simple-typing">
      <Loader2 className="spin" size={16} />
      Thinking
    </div>
  );
}

function PresetPrompts({ prompts, onSelect }: { prompts: string[]; onSelect: (prompt: string) => void }) {
  return (
    <div className="simple-presets">
      <h2>What should we research?</h2>
      <div>
        {prompts.map((prompt) => (
          <button key={prompt} onClick={() => onSelect(prompt)} type="button">
            {prompt}
          </button>
        ))}
      </div>
    </div>
  );
}

function ChatInput({
  disabled,
  draft,
  loading,
  onChange,
  onStop,
  onSubmit,
}: {
  disabled: boolean;
  draft: string;
  loading: boolean;
  onChange: (value: string) => void;
  onStop: () => void;
  onSubmit: () => void;
}) {
  return (
    <form
      className="simple-composer"
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit();
      }}
    >
      <textarea
        value={draft}
        onChange={(event) => onChange(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            onSubmit();
          }
        }}
        placeholder="Message Deep Research..."
        rows={1}
      />
      <button disabled={!draft.trim() || disabled} type="submit" title="Send">
        {loading ? <Loader2 className="spin" size={18} /> : <Send size={18} />}
      </button>
      {loading && (
        <button className="simple-stop" onClick={onStop} type="button" title="Stop">
          <Square size={16} />
        </button>
      )}
    </form>
  );
}
