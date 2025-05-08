"""Metaflow example demonstrating parallel branching and joining."""

from metaflow import FlowSpec, step


class ParallelOpsFlow(FlowSpec):
    """A flow that demonstrates parallel processing with branching and joining."""

    @step
    def start(self):
        """Initialize the artifact with a numerical value."""
        self.artifact = 10
        print(f"Starting with artifact value: {self.artifact}")
        # Branch out to both add_constant and multiply_constant steps
        self.next(self.add_constant, self.multiply_constant)

    @step
    def add_constant(self):
        """Add a constant to the artifact."""
        self.constant = 5
        self.result = self.artifact + self.constant
        print(f"Added {self.constant} to {self.artifact}, result: {self.result}")
        self.next(self.join)

    @step
    def multiply_constant(self):
        """Multiply the artifact by a constant."""
        self.constant = 2
        self.result = self.artifact * self.constant
        print(f"Multiplied {self.artifact} by {self.constant}, result: {self.result}")
        self.next(self.join)

    @step
    def join(self, inputs):
        """Join the parallel branches and compute the sum of results."""
        # Gather results from both branches
        self.add_result = inputs.add_constant.result
        self.multiply_result = inputs.multiply_constant.result

        # Print both branch outcomes
        print(f"Addition branch result: {self.add_result}")
        print(f"Multiplication branch result: {self.multiply_result}")

        # Compute the sum of the two outcomes
        self.final_sum = self.add_result + self.multiply_result
        print(f"Sum of both branch outcomes: {self.final_sum}")

        self.next(self.end)

    @step
    def end(self):
        """End of the flow."""
        print("Flow completed successfully!")


if __name__ == "__main__":
    ParallelOpsFlow()
