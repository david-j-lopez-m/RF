from retriever import AlertRetriever
from rag import answer_query_with_rag

def main():
    retriever = AlertRetriever()
    while True:
        q = input("Enter your search query (or 'exit'): ").strip()
        if q.lower() == 'exit':
            break
        results = retriever.search(q)
        for i, res in enumerate(results, 1):
            print(f"{i}. {res}")

        use_rag = input("Do you want an LLM answer using RAG? (y/n): ").strip().lower()
        if use_rag == 'y':
            answer = answer_query_with_rag(q)
            print(f"LLM Answer:\n{answer}")

if __name__ == "__main__":
    main()