import requests

def call_llm(prompt, model="tinyllama"):
    """
    Calls a local LLM running with Ollama. Returns the generated answer as a string.
    """
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False  # True streams chunks, False gives one string
    }
    try:
        response = requests.post(url, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        return data["response"].strip()
    except Exception as e:
        print(f"LLM call failed: {e}")
        return "[LLM ERROR]"

# Optional: test directly
if __name__ == "__main__":
    print(call_llm("What is the capital of Spain?"))