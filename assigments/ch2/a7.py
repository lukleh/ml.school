"""Comparison of @environment decorator with python-dotenv."""

import os

from metaflow import FlowSpec, current, environment, step


class EnvironmentVarsComparisonFlow(FlowSpec):
    """Compares @environment decorator with python-dotenv for env variables."""

    @step
    def start(self):
        """Start the flow."""
        print(f"Starting flow: {current.flow_name}")
        self.next(self.environment_decorator_approach)

    @environment(
        vars={
            "ENV_KERAS_BACKEND": f"tensorflow-{os.getenv('METAFLOW_VARIABLE')}",
            "ENV_MLFLOW_URI": "http://metaflow.example.com:5000",
        }
    )
    @step
    def environment_decorator_approach(self):
        """Demonstrate Metaflow's @environment decorator."""
        print("\n=== METAFLOW @ENVIRONMENT APPROACH ===")
        print(f"ENV_KERAS_BACKEND: {os.getenv('ENV_KERAS_BACKEND')}")
        print(f"ENV_MLFLOW_URI: {os.getenv('ENV_MLFLOW_URI')}")
        print(f"METAFLOW_VARIABLE: {os.getenv('METAFLOW_VARIABLE')}")

        # Store results for comparison
        self.environment_vars = {
            "ENV_KERAS_BACKEND": os.getenv("ENV_KERAS_BACKEND"),
            "ENV_MLFLOW_URI": os.getenv("ENV_MLFLOW_URI"),
        }

        self.next(self.dotenv_approach)

    @step
    def dotenv_approach(self):
        """Demonstrate python-dotenv for loading environment variables."""
        print("\n=== PYTHON-DOTENV APPROACH ===")

        from dotenv import dotenv_values, load_dotenv

        # Load .env file into environment
        load_dotenv()

        # Also load as dictionary
        dotenv_vars = dotenv_values(".env")
        print(f"Variables from .env: {dotenv_vars}")

        # Show actual values loaded into environment
        for key in dotenv_vars:
            print(f"{key}: {os.getenv(key)}")

        self.dotenv_vars = dotenv_vars

        self.next(self.compare_approaches)

    @step
    def compare_approaches(self):
        """Compare the two approaches."""
        print("\n=== COMPARISON SUMMARY ===")

        print("\n1. Metaflow @environment:")
        for key, value in self.environment_vars.items():
            print(f"  {key}: {value}")

        print("\n2. python-dotenv (.env file):")
        if isinstance(self.dotenv_vars, dict):
            for key, value in self.dotenv_vars.items():
                print(f"  {key}: {value}")

        print("\nKey Differences:")
        print(
            "- @environment: Defined in code, tracked by Metaflow, "
            "ideal for remote execution"
        )
        print("- dotenv: External configuration, better separation of code and config")

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        print(
            "Flow completed. Run with: "
            "METAFLOW_VARIABLE=123 uv run assigments/ch2/a7.py run"
        )


if __name__ == "__main__":
    EnvironmentVarsComparisonFlow()
