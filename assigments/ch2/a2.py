"""Arithmetic operations tracking flow."""

from metaflow import FlowSpec, step


class ArithmeticFlow(FlowSpec):
    """A flow that tracks a sequence of numerical operations."""

    @step
    def start(self):
        """Initialize our numerical artifact with a starting value."""
        self.number = 5
        self.history = [self.number]
        self.next(self.add_step)

    @step
    def add_step(self):
        """Add 10 to the current number."""
        self.number += 10
        self.history.append(self.number)
        self.next(self.subtract_step)

    @step
    def subtract_step(self):
        """Subtract 3 from the current number."""
        self.number -= 3
        self.history.append(self.number)
        self.next(self.multiply_step)

    @step
    def multiply_step(self):
        """Multiply the current number by 2."""
        self.number *= 2
        self.history.append(self.number)
        self.next(self.end)

    @step
    def end(self):
        """Print the history of values and calculate sum and average."""
        print("History of values:", self.history)
        total_sum = sum(self.history)
        average = total_sum / len(self.history)
        print(f"Sum of all values: {total_sum}")
        print(f"Average of all values: {average}")


if __name__ == "__main__":
    ArithmeticFlow()
