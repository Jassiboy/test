"""
Mini Project 2: RAG Chatbot over your own documents (PDF/txt/md)
===================================================================

Pipeline: Load docs -> Chunk -> Embed -> Vector store (FAISS)
          -> [Query time] Retrieve top-k -> Assemble prompt -> LLM -> Grounded answer

Setup:
    pip install langchain langchain-openai langchain-community faiss-cpu pypdf
    set OPENAI_API_KEY=sk-...

Run:
    python 02_rag_chatbot.py ./docs "What does the doc say about eventual consistency?"

`./docs` should be a folder containing your PDFs/markdown/text files
(e.g., point it at your Databricks/DDIA notes folder).
"""

import sys
from pathlib import Path

from langchain_community.document_loaders import DirectoryLoader, PyPDFLoader, TextLoader
from langchain_community.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

RAG_PROMPT = ChatPromptTemplate.from_template(
    "You are a documentation assistant. Answer the question using ONLY the context below.\n"
    "If the answer is not present in the context, say: \"I don't have enough information.\"\n"
    "Cite the source file for each claim in square brackets.\n\n"
    "Context:\n{context}\n\nQuestion: {question}\n\nAnswer:"
)


def load_documents(folder: str):
    pdf_loader = DirectoryLoader(folder, glob="**/*.pdf", loader_cls=PyPDFLoader)
    txt_loader = DirectoryLoader(folder, glob="**/*.{txt,md}", loader_cls=TextLoader)
    return pdf_loader.load() + txt_loader.load()


def format_docs(docs) -> str:
    return "\n\n".join(f"[{Path(d.metadata.get('source', 'unknown')).name}] {d.page_content}" for d in docs)


def build_rag_chain(folder: str):
    docs = load_documents(folder)
    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)
    chunks = splitter.split_documents(docs)

    vectorstore = FAISS.from_documents(chunks, OpenAIEmbeddings())
    retriever = vectorstore.as_retriever(search_kwargs={"k": 4})

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    return (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | RAG_PROMPT
        | llm
        | StrOutputParser()
    )


def main():
    if len(sys.argv) < 3:
        print("Usage: python 02_rag_chatbot.py <docs_folder> <question>")
        return

    folder, question = sys.argv[1], " ".join(sys.argv[2:])
    chain = build_rag_chain(folder)
    answer = chain.invoke(question)
    print(f"Q: {question}\n\nA: {answer}")


if __name__ == "__main__":
    main()
