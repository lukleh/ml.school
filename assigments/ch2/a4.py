"""Module for squaring numbers using Metaflow."""
from metaflow import FlowSpec, Parameter, step


class SquareNumbersFlow(FlowSpec):
    """A flow that squares each number in a list using foreach."""

    numbers = Parameter("numbers",
                        help="Comma-separated list of numbers to square",
                        default="1,2,3,4,5")

    @step
    def start(self):
        """Initialize the flow and prepare the numbers for processing."""
        # Convert the input string parameter to a list of integers
        self.number_list = [int(n.strip()) for n in self.numbers.split(",")]
        print(f"Input numbers: {self.number_list}")

        # Foreach will process each element in parallel
        self.next(self.square, foreach="number_list")

    @step
    def square(self):
        """Square the current number in this foreach branch."""
        # Get the current item being processed in this foreach branch
        self.input_number = self.input
        # Calculate the square of the input number
        self.squared_result = self.input_number ** 2
        print(f"Squaring {self.input_number} = {self.squared_result}")

        self.next(self.join)

    @step
    def join(self, inputs):
        """Collect all squared results and calculate the sum."""
        # Collect all the squared results into a list
        self.squared_numbers = [inp.squared_result for inp in inputs]
        self.original_numbers = [inp.input_number for inp in inputs]

        # Calculate the sum of squared numbers
        self.squared_sum = sum(self.squared_numbers)

        # Print the results
        print("\nResults:")
        print(f"Original numbers: {self.original_numbers}")
        print(f"Squared numbers: {self.squared_numbers}")
        print(f"Sum of squared numbers: {self.squared_sum}")

        self.next(self.end)

    @step
    def end(self):
        """End the flow and report completion."""
        print("Flow completed successfully.")


if __name__ == "__main__":
    SquareNumbersFlow()
