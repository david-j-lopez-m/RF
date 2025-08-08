from retriever import AlertRetriever
from llm_interface import call_llm  # You'll need to implement this function

def build_rag_prompt(query, docs):
    context = "\n".join([
        f"- [{doc.get('event_datetime')}] {doc.get('title')}: {doc.get('description')} (Severity: {doc.get('severity')})"
        for doc in docs
    ])
    prompt = (
        f"You are an expert emergency alert assistant.\n"
        f"User query: {query}\n\n"
        f"Here are the most relevant recent alerts:\n{context}\n\n"
        "Please provide a clear answer, referencing the alerts above when possible."
    )
    return prompt

def answer_query_with_rag(query):
    """
    Main entry point: embeds the query, retrieves relevant docs, builds a prompt,
    calls the LLM, and returns the generated answer.
    """
    retriever = AlertRetriever()
    context_docs = retriever.search(query)
    prompt = build_rag_prompt(query, context_docs)
    answer = call_llm(prompt)
    return answer
