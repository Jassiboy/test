# 06. Interview Question Bank — LLM, LangChain, RAG

## LLM Fundamentals

1. **What is an LLM, at a mechanical level (what is it actually predicting)?**
   > A neural network (Transformer decoder) trained to predict the probability distribution of the next token given prior tokens; text generation = repeated sampling from that distribution.

2. **LLM vs traditional ML — what's fundamentally different?**
   > Traditional ML: narrow, supervised, structured features → label, trained per-task. LLM: broad, self-supervised pretraining on raw text, then adapted (prompting/fine-tuning) to many tasks without retraining from scratch.

3. **What is a token, and why doesn't 1 token = 1 word?**
   > Sub-word unit from a tokenizer (e.g., BPE). Words split into common sub-word pieces; rare words/code/non-English use more tokens per word. Cost and limits are token-based.

4. **What is the context window, and what happens if you exceed it?**
   > Max tokens (input+output) the model can process in one call. Exceeding it truncates/drops content or errors — this is a core motivation for RAG (retrieve only relevant chunks instead of dumping everything in).

5. **Explain temperature and when you'd set it to 0.**
   > Controls randomness of sampling by scaling logits before softmax. `0` = deterministic/greedy — use for structured extraction, code gen, RAG answers where reproducibility matters. Higher = more creative/diverse, for brainstorming.

6. **What's the difference between top-p and top-k sampling?**
   > top-k restricts sampling to the k most probable tokens; top-p (nucleus) restricts to the smallest set of tokens whose cumulative probability ≥ p — adaptive to the shape of the distribution.

7. **What are system, user, and assistant roles for?**
   > System sets persistent behavior/persona/rules; user is the human input; assistant is the model's own prior turns (for multi-turn context). Some APIs add a tool/function role for tool results.

8. **What causes hallucination, and how do you mitigate it?**
   > Model generates statistically plausible but unverified text since it has no built-in fact-checking. Mitigate via grounding (RAG), explicit "say I don't know" instructions, structured output + validation, and evaluation (faithfulness scoring).

9. **What is few-shot prompting, and when would you use it over zero-shot?**
   > Providing example input/output pairs in the prompt to demonstrate the desired format/behavior. Use when the task's format is unusual or the model underperforms zero-shot.

10. **What is chain-of-thought prompting, and why does it help?**
    > Instructing the model to reason step-by-step before the final answer; improves performance on multi-step reasoning/math because it "spends more computation" per answer via explicit intermediate tokens.

11. **How would you force reliable structured (JSON) output from an LLM?**
    > Prefer native structured-output/tool-calling APIs (`with_structured_output`, `response_format=json_schema`) over prompt-only "please output JSON"; validate with Pydantic; add retry/repair logic for parse failures.

12. **What's a key security risk when feeding external/untrusted text (web pages, documents) into an LLM prompt?**
    > Prompt injection — malicious instructions embedded in the content can hijack the model's behavior. Mitigate by treating retrieved content as data, not instructions, and by sandboxing tool execution/output.

## LangChain

13. **What problem does LangChain solve that raw API calls don't?**
    > Standardizes interfaces across providers, provides composable pipelines (LCEL) with built-in streaming/batching/async, output parsing, retrieval/vector store integrations, and observability — reduces boilerplate for multi-step LLM apps.

14. **Name the core LangChain abstractions.**
    > Models (Chat/LLM/Embeddings), Prompts (templates), Messages (System/Human/AI/Tool), Output Parsers, Retrievers, Tools, Chains/Runnables, Memory, Agents.

15. **What is a Runnable, and why does everything in modern LangChain implement it?**
    > A uniform interface (`invoke`/`batch`/`stream`/`ainvoke`) that any pipeline step implements, enabling composition via `|` (LCEL) with streaming/async/batch support automatically inherited by the whole pipeline.

16. **Chain vs Runnable — what's the actual difference?**
    > "Chain" is the concept of a multi-step pipeline; legacy chains (e.g., `LLMChain`) were class-based and less composable/streamable. "Runnable"/LCEL is the modern declarative mechanism (`prompt | llm | parser`) that implements chains with built-in streaming, batching, and async — the current recommended approach.

17. **Why use LangChain's output parsers instead of `json.loads()` on the raw response?**
    > Built-in schema validation (Pydantic), automatic prompt formatting instructions, and (with `with_structured_output`) leveraging the provider's native structured-output guarantees rather than hoping the model's free text is valid JSON.

18. **How does LangChain's tool calling actually work end-to-end?**
    > You bind tool schemas to the model (`llm.bind_tools([...])`); the model doesn't execute code, it returns a structured `tool_calls` proposal; your application code executes the real function and returns the result as a `ToolMessage`, which is fed back to the model for the final answer.

19. **What's the benefit of LCEL's `|` composition over manually calling `.invoke()` on each step yourself?**
    > Automatic streaming/batch/async across the whole pipeline, easier testing/swapping of individual steps, support for `RunnableParallel`/`RunnableBranch`/`RunnableLambda` to build more complex flows declaratively.

## RAG

20. **What problem does RAG solve that a bigger context window doesn't?**
    > Access to private/updated/very large knowledge bases beyond what fits (or is efficient/reliable) in a context window, plus reduced hallucination via grounding — bigger windows also suffer "lost in the middle" attention issues.

21. **Walk through a basic RAG pipeline end to end.**
    > Offline: load docs → chunk → embed → store in vector DB. Online: embed query → similarity search top-k → assemble prompt with retrieved context → LLM generates grounded answer (ideally with citations).

22. **How do you choose chunk size and overlap?**
    > Start ~500-1000 tokens with 10-20% overlap; tune empirically via retrieval eval (context precision/recall) — too small loses context, too large adds noise and increases cost.

23. **What is re-ranking, and why not just use the top-k from vector search directly?**
    > Vector (bi-encoder) search is fast but approximate since query and doc are embedded independently; a cross-encoder re-ranker scores (query, doc) pairs jointly for higher precision on a smaller candidate set (e.g., top 50 → top 5).

24. **What is hybrid search, and when is it necessary?**
    > Combining dense vector similarity with sparse keyword search (e.g., BM25); necessary when exact terms matter (IDs, codes, acronyms, names) that embeddings alone may not match well.

25. **How would you evaluate a RAG system?**
    > Build a golden Q&A set; measure context precision/recall (retrieval quality) and faithfulness/answer relevance (generation quality), e.g., with Ragas; track regressions over time as prompts/retrieval change.

26. **What causes RAG hallucination even with retrieved context, and how do you reduce it?**
    > Model ignoring/misusing context, insufficient/irrelevant retrieved chunks, or no explicit grounding instruction. Reduce via explicit "answer only from context" prompting, better retrieval (re-ranking, hybrid search), and faithfulness evaluation with regression tests.

27. **What is agentic RAG / corrective RAG?**
    > Agentic RAG: an LLM agent decides when/what to retrieve and can loop/reformulate queries rather than a single fixed retrieval step. Corrective RAG adds a self-grading step that checks retrieved doc relevance and falls back (e.g., to web search or query rewrite) if retrieval quality is low.

28. **When would you NOT use RAG, and consider fine-tuning instead?**
    > When the need is to change the model's *style/behavior/format* consistently (not inject facts), when latency from retrieval is unacceptable, or when the knowledge is small/stable enough to bake in — RAG is generally preferred for large/frequently-changing factual knowledge because it's cheaper to update and more auditable (cite sources) than fine-tuning.

---
⬅ Back: [05_Agents_and_Tools.md](05_Agents_and_Tools.md) | Back to [00_README.md](00_README.md)
