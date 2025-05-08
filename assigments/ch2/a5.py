"""Flow demonstrating the retry decorator for handling flaky services."""

import random

from metaflow import FlowSpec, Parameter, current, retry, step


class RetryFlow(FlowSpec):
    """A flow that demonstrates the use of the @retry decorator."""

    num_jobs = Parameter(
        "num_jobs", help="Number of times to run the flaky service", default=5
    )

    @step
    def start(self):
        """Start the flow."""
        print(f"Starting run: {current.run_id}")

        # Create a list of jobs to run in parallel
        self.jobs = list(range(self.num_jobs))
        print(f"Will run flaky service {self.num_jobs} times")

        self.next(self.flaky_service, foreach="jobs")

    @retry(times=3)
    @step
    def flaky_service(self):
        """Simulate a flaky external service with a high failure rate.

        The @retry decorator will cause this step to be retried up to 3 times.
        """
        print(f"Running service call {self.input} in attempt {current.retry_count}...")

        # Define success probability
        success_probability = 0.5  # 50% chance

        # Simulate a flaky service that fails 50% of the time
        if random.random() < success_probability:
            print(f"Service call {self.input} succeeded!")
            self.result = "SUCCESS"
        else:
            print(f"Service call {self.input} failed! Will retry...")
            error_message = "External service failed"
            raise ServiceError(error_message)

        self.next(self.join)

    @step
    def join(self, inputs):
        """Join the parallel steps and collect results."""
        # Collect results from all parallel runs
        self.results = [inp.result for inp in inputs]
        print(f"Collected {len(self.results)} successful service call results")

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        print("Flow completed successfully!")
        print(f"Service call results: {self.results}")


class ServiceError(Exception):
    """Custom exception for service failures."""


if __name__ == "__main__":
    RetryFlow()
