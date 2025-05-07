"""Prompt response workflow using Metaflow and Ollama LLM with card visualization."""

import json
from datetime import UTC, datetime

import requests
from metaflow import FlowSpec, Parameter, card, step


class PromptResponseFlow(FlowSpec):
    """Flow that generates LLM responses to prompts and visualizes them.

    1. Takes a text prompt from the user
    2. Generates a response using Ollama LLM
    3. Stores the response as an artifact
    4. Creates a custom card visualization
    """

    prompt = Parameter(
        "prompt",
        help="The text prompt to send to the LLM",
        default="""You are a helpful assistant that always responds in a
                structured JSON format with separate "thought" and "answer" fields.
                Tell me a short joke about programming"""
    )

    model = Parameter("model",
                     help="The LLM model to use",
                     default="qwen3:8b")

    @step
    def start(self):
        """Start the flow."""
        print(f"Starting with prompt: {self.prompt}")
        self.next(self.generate_response)

    @step
    def generate_response(self):
        """Generate a response using the Ollama LLM."""
        # Store the timestamp
        self.timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")

        # Make API call to the local Ollama instance
        response = requests.post(
            "http://host.docker.internal:11434/api/generate",
            json={
                "model": self.model,
                "prompt": self.prompt,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.8}
            },
            timeout=60  # Add timeout for safety
        )
        response.raise_for_status()

        # Extract generated text
        llm_response = response.json()
        self.response_data = llm_response.get("response", "")
        self.response = json.loads(self.response_data)["answer"]

        print(f"Generated response: {self.response}")
        self.next(self.create_card)

    @card(type="html")
    @step
    def create_card(self):
        """Create a custom card to visualize the prompt and response."""
        # Build HTML for the card - similar to the pattern in a6.py and cards.py
        css = """
            .container {font-family:Arial,sans-serif; line-height:1.6;
                max-width:800px; margin:0 auto; padding:20px;}
            .card {background-color:white; border-radius:8px; padding:20px;
                box-shadow:0 2px 4px rgba(0,0,0,0.1);}
            .header {border-bottom:1px solid #eee; padding-bottom:10px;
                margin-bottom:20px;}
            .timestamp {color:#888; font-size:0.8em;}
            .section {margin-bottom:20px;}
            .label {font-weight:bold; color:#555; margin-bottom:5px;}
            .content {background-color:#f9f9f9; border-left:3px solid #2196F3;
                padding:10px; border-radius:3px; white-space:pre-wrap;}
            .footer {font-size:0.8em; color:#888; text-align:right;
                margin-top:20px;}
        """

        self.html = f"""
        <style>{css}</style>
        <div class="container">
            <div class="card">
                <div class="header">
                    <h2>LLM Prompt Response</h2>
                    <div class="timestamp">{self.timestamp}</div>
                </div>
                <div class="section">
                    <div class="label">Prompt:</div>
                    <div class="content">{self.prompt}</div>
                </div>
                <div class="section">
                    <div class="label">Response:</div>
                    <div class="content">{self.response}</div>
                </div>
                <div class="footer">
                    Model: {self.model}
                </div>
            </div>
        </div>
        """

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        print("Flow completed successfully!")
        print(f"Prompt: {self.prompt}")
        print(f"Response: {self.response}")
        print("\nTo view the card, run:")
        print("  uv run -- python a10.py card server")


if __name__ == "__main__":
    PromptResponseFlow()
