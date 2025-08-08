from retriever import AlertRetriever
from rag import answer_query_with_rag

def main():
    retriever = AlertRetriever()
    queries = [
        "Are there any severe flood alerts in Spain?",
        "Are there any dangerous wildfires in Spain?",
        "What is the current earthquake risk in Madrid?",
        "Are there any relevant space weather alerts with potential ground effects?",
        "Are there any Coronal Mass Ejection, Flare, or Radiation Belt Enhancement events considered relevant?",
        "Summarize all active and relevant natural disaster alerts for Spain."
    ]
    for q in queries:
        print(f"\nRunning query: {q}")

        # Optionally retrieve documents but skip printing
        results = retriever.search(q)
        # Uncomment below if you want to see raw docs for debugging
        # for i, res in enumerate(results, 1):
        #     print(f"{i}. {res}")

        answer = answer_query_with_rag(q)
        print(f"LLM Answer:\n{answer}\n")

if __name__ == "__main__":
    main()