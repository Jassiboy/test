Phase 4: RAG (Retrieval-Augmented Generation) (~30-40 hrs)
Depends on Phase 3

Objectives: Build a production-shaped RAG pipeline, not just a toy demo.

Topics:

Ingestion pipelines, chunk size/overlap tuning, retrieval (top-k, MMR), re-ranking (cross-encoders)
Prompt assembly with retrieved context, citation/source tracking, hallucination mitigation
RAG evaluation (faithfulness, answer relevance, context precision/recall)
Resources:

LangChain or LlamaIndex official docs + tutorials (pick one as primary)
DeepLearning.AI "LangChain for LLM Application Development" and "Building and Evaluating Advanced RAG"
Ragas library docs for evaluation
Hands-on / Project A: RAG chatbot over your Databricks Cookbook, DDIA, and SQL notes PDFs — must answer questions with cited page/source, handle "I don't know" gracefully, and include a basic eval script (Ragas or manual golden Q&A set).

Deliverable: RAG chatbot repo with README, architecture diagram, and eval results table.
