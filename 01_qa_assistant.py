"""
Mini Project 1: Technical Q&A Assistant (structured output)
=============================================================

Pipeline:  User Question -> Prompt Template -> LLM -> Structured Response

Setup:
    pip install langchain langchain-openai pydantic
    set OPENAI_API_KEY=sk-...      (PowerShell: $env:OPENAI_API_KEY="sk-...")

Run:
    python 01_qa_assistant.py "What is a database index?"

To use a free local model instead of OpenAI, swap ChatOpenAI for ChatOllama
(pip install langchain-ollama, run `ollama pull llama3`), keeping the rest identical.
"""

import sys

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field


class QAResponse(BaseModel):
    """Structured shape we want back from the LLM for every answer."""

    answer: str = Field(description="Direct, concise answer to the question")
    explanation: str = Field(description="1-3 sentence supporting explanation")
    difficulty: str = Field(description="One of: beginner, intermediate, advanced")
    confidence: float = Field(description="Model's confidence in the answer, 0-1")


PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are a precise technical Q&A assistant for software/data engineering topics. "
            "Answer clearly and rate your own confidence honestly.",
        ),
        ("human", "{question}"),
    ]
)


def build_chain():
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    structured_llm = llm.with_structured_output(QAResponse)
    return PROMPT | structured_llm


def main():
    question = " ".join(sys.argv[1:]) or "What is the difference between a list and a tuple in Python?"
    chain = build_chain()
    result: QAResponse = chain.invoke({"question": question})

    print(f"Q: {question}\n")
    print(f"Answer      : {result.answer}")
    print(f"Explanation : {result.explanation}")
    print(f"Difficulty  : {result.difficulty}")
    print(f"Confidence  : {result.confidence:.2f}")


if __name__ == "__main__":
    main()
