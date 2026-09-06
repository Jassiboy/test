"""
Mini Project 4: SQL Agent (tool calling + guardrails)
========================================================

Given a natural-language question, the agent decides to call a read-only
SQL tool against a local SQLite database, then summarizes the result.

Demonstrates:
  - Tool definition + binding
  - ReAct-style loop (LLM decides -> tool executes -> LLM answers)
  - Guardrails: read-only enforcement, row limit, max iterations

Setup:
    pip install langchain langchain-openai
    set OPENAI_API_KEY=sk-...

Run:
    python 04_sql_agent.py "How many customers are in the database?"

Uses an in-memory SQLite demo DB seeded with a tiny customers table so it
runs with zero external setup.
"""

import sqlite3
import sys

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

MAX_ITERATIONS = 4

# --- Demo database setup (in-memory, seeded) -------------------------------
_conn = sqlite3.connect(":memory:", check_same_thread=False)
_conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)")
_conn.executemany(
    "INSERT INTO customers (name, region) VALUES (?, ?)",
    [("Acme Corp", "US"), ("Globex", "EU"), ("Initech", "US"), ("Umbrella", "APAC")],
)
_conn.commit()


@tool
def run_read_only_sql(query: str) -> str:
    """Execute a READ-ONLY SQL SELECT query against the customers database and return rows."""
    normalized = query.strip().lower()
    if not normalized.startswith("select"):
        return "Error: only SELECT statements are allowed."
    try:
        cursor = _conn.execute(query)
        rows = cursor.fetchmany(50)  # guardrail: cap rows returned
        return str(rows) if rows else "No rows returned."
    except sqlite3.Error as e:
        return f"SQL error: {e}"


def run_agent(question: str) -> str:
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    llm_with_tools = llm.bind_tools([run_read_only_sql])

    messages = [
        SystemMessage(
            "You answer questions about a 'customers' table (columns: id, name, region) "
            "using the run_read_only_sql tool. Only ever issue SELECT statements."
        ),
        HumanMessage(question),
    ]

    for _ in range(MAX_ITERATIONS):  # guardrail: cap agent loop iterations
        response = llm_with_tools.invoke(messages)
        messages.append(response)

        if not response.tool_calls:
            return response.content  # final answer, no more tool calls needed

        for call in response.tool_calls:
            result = run_read_only_sql.invoke(call["args"])
            messages.append(ToolMessage(content=str(result), tool_call_id=call["id"]))

    return "Stopped: exceeded max iterations without a final answer."


def main():
    question = " ".join(sys.argv[1:]) or "How many customers are in the US region?"
    print(f"Q: {question}\n")
    print(f"A: {run_agent(question)}")


if __name__ == "__main__":
    main()
