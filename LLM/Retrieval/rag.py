from retriever import AlertRetriever
from llm_interface import call_llm  # You'll need to implement this function

def build_rag_prompt(query, docs):
    """
    Builds a prompt for the LLM given the user query and retrieved docs.
    """
    context = "\n\n".join(
        [f"- {doc['title']}: {doc['description']}" for doc in docs]
    )
    prompt = (
        f"User query: {query}\n"
        f"Relevant alerts:\n{context}\n"
        "Based on the information above, answer the user's query as clearly and concisely as possible."
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
