"""Student processing workflow using Metaflow and Ollama LLM."""

import json

import requests
from metaflow import FlowSpec, step


class StudentProcessingFlow(FlowSpec):
    """Flow that processes student data.

    1. Generates student data using Ollama LLM
    2. Processes each student in parallel (uppercase name, increase score)
    3. Aggregates results
    """

    @step
    def start(self):
        """Start the flow."""
        print("Starting the flow...")
        self.next(self.generate_student_data)

    @step
    def generate_student_data(self):
        """Generate list of student dictionaries using Ollama LLM."""
        # Prompt for the LLM
        prompt = """
        Generate a JSON list of 5 students, where each student has the following
        attributes:
        - name (string): A realistic student name
        - score (number): A random score between 50 and 95

        Format the response as a valid JSON list of dictionaries, with no other text.
        Example format:
        [
            {"name": "John Smith", "score": 82},
            {"name": "Maria Garcia", "score": 91}
        ]
        """

        # Make API call to the local Ollama instance
        response = requests.post(
            "http://host.docker.internal:11434/api/generate",
            json={
                "model": "qwen3:8b",
                "prompt": prompt,
                "stream": False,
                "format": "json",
            },
            timeout=60,  # Add timeout for safety
        )
        response.raise_for_status()

        # Extract generated text and parse as JSON
        llm_response = response.json().get("response", "")
        self.students = json.loads(llm_response)["students"]

        print(f"Generated {len(self.students)} student records")

        # Start a branch for each student
        self.next(self.process_student, foreach="students")

    @step
    def process_student(self):
        """Process an individual student: uppercase name and increase score."""
        # Get the current student from the foreach loop
        self.student = dict(self.input)

        # Transform name to uppercase
        self.student["name"] = self.student["name"].upper()

        # Increase score by 10 points
        self.student["score"] += 10

        # Ensure score doesn't exceed 100
        self.student["score"] = min(self.student["score"], 100)

        print(f"Processed student: {self.student}")
        self.next(self.join_results)

    @step
    def join_results(self, inputs):
        """Join all parallel branches and aggregate results."""
        # Collect processed students from all branches
        self.processed_students = [inp.student for inp in inputs]

        # Calculate aggregate statistics
        self.total_score = sum(student["score"] for student in self.processed_students)
        self.average_score = self.total_score / len(self.processed_students)

        print("\nProcessed students:")
        for student in self.processed_students:
            print(f"- {student['name']}: {student['score']}")

        print("\nAggregate results:")
        print(f"Total students: {len(self.processed_students)}")
        print(f"Total score: {self.total_score}")
        print(f"Average score: {self.average_score:.2f}")

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        print("Flow completed successfully!")


if __name__ == "__main__":
    StudentProcessingFlow()
