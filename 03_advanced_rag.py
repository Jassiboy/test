"""
Mini Project 3: Advanced RAG - hybrid search + cross-encoder re-ranking + citations
=====================================================================================

Adds on top of the basic RAG chatbot (02_rag_chatbot.py):
  1. Hybrid retrieval  : vector search (FAISS) + keyword search (BM25), merged
  2. Re-ranking        : cross-encoder re-scores merged candidates for precision
  3. Citation-aware answer with explicit source list

Setup:
    pip install langchain langchain-openai langchain-community faiss-cpu pypdf \
                rank_bm25 sentence-transformers

Run:
    python 03_advanced_rag.py ./docs "What is eventual consistency?"
"""

import sys
from pathlib import Path

from langchain_community.document_loaders import DirectoryLoader, PyPDFLoader, TextLoader
from langchain_community.retrievers import BM25Retriever
from langchain_community.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import CrossEncoder

RERANKER = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

PROMPT = ChatPromptTemplate.from_template(
    "Answer using ONLY the context. Cite sources like [filename] after each claim.\n"
    "If insufficient information, say so explicitly.\n\n"
    "Context:\n{context}\n\nQuestion: {question}\n\nAnswer:"
)


def load_and_split(folder: str):
    docs = (
        DirectoryLoader(folder, glob="**/*.pdf", loader_cls=PyPDFLoader).load()
        + DirectoryLoader(folder, glob="**/*.{txt,md}", loader_cls=TextLoader).load()
    )
    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)
    return splitter.split_documents(docs)


def hybrid_retrieve(query: str, chunks, vectorstore, k_each: int = 10):
    """Merge dense (vector) and sparse (BM25 keyword) retrieval results."""
    vector_hits = vectorstore.similarity_search(query, k=k_each)

    bm25 = BM25Retriever.from_documents(chunks)
    bm25.k = k_each
    keyword_hits = bm25.invoke(query)

    seen, merged = set(), []
    for doc in vector_hits + keyword_hits:
        key = doc.page_content[:100]
        if key not in seen:
            seen.add(key)
            merged.append(doc)
    return merged


def rerank(query: str, candidates, top_n: int = 5):
    pairs = [(query, d.page_content) for d in candidates]
    scores = RERANKER.predict(pairs)
    ranked = [d for _, d in sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)]
    return ranked[:top_n]


def format_docs(docs) -> str:
    return "\n\n".join(f"[{Path(d.metadata.get('source', 'unknown')).name}] {d.page_content}" for d in docs)


def main():
    if len(sys.argv) < 3:
        print("Usage: python 03_advanced_rag.py <docs_folder> <question>")
        return

    folder, question = sys.argv[1], " ".join(sys.argv[2:])

    chunks = load_and_split(folder)
    vectorstore = FAISS.from_documents(chunks, OpenAIEmbeddings())

    candidates = hybrid_retrieve(question, chunks, vectorstore)
    top_chunks = rerank(question, candidates)

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    chain = PROMPT | llm | StrOutputParser()
    answer = chain.invoke({"context": format_docs(top_chunks), "question": question})

    sources = sorted({Path(d.metadata.get("source", "unknown")).name for d in top_chunks})
    print(f"Q: {question}\n\nA: {answer}\n\nSources used: {', '.join(sources)}")


if __name__ == "__main__":
    main()
