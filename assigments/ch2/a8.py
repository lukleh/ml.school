"""CSV Processing Flow module for Metaflow.

This module contains a Metaflow flow that processes CSV files and reports information
about their structure and content, with robust error handling.
"""

import csv
from io import StringIO

from metaflow import FlowSpec, IncludeFile, step


class CSVProcessingFlow(FlowSpec):
    """A flow that loads a CSV file and reports information about its structure.

    This flow demonstrates how to use IncludeFile to load external files and
    how to implement robust error handling for file processing.
    """

    # Include a CSV file as a parameter
    csv_file = IncludeFile(
        "csv_file",
        help="CSV file to process",
        default="../../data/sample.csv",  # Updated path
    )

    @step
    def start(self):
        """Start the flow and process the included CSV file."""
        print("Starting CSV processing flow")
        self.next(self.process_csv)

    def _validate_csv_content(self, csv_content):
        """Validate the CSV content before processing.

        Args:
            csv_content: The CSV content to validate.

        Raises:
            ValueError: If the CSV content is empty.

        """
        # Check if file is empty
        if not csv_content.strip():
            error_msg = "The CSV file is empty"
            raise ValueError(error_msg)

    def _check_rows_consistency(self, reader, header):
        """Process CSV rows and check for consistency.

        Args:
            reader: CSV reader object.
            header: The CSV header row.

        Returns:
            tuple: Contains row_count, inconsistent_rows, min_cols, max_cols.

        Raises:
            StopIteration: If the reader has no data rows.

        """
        csv_data = []
        self.column_count = len(header)
        self.column_names = header

        # Read all rows
        row_count = 0
        inconsistent_rows = 0
        min_cols = self.column_count
        max_cols = self.column_count

        for row in reader:
            csv_data.append(row)
            row_count += 1

            # Check for inconsistent row lengths
            row_len = len(row)
            if row_len != self.column_count:
                inconsistent_rows += 1

            min_cols = min(min_cols, row_len)
            max_cols = max(max_cols, row_len)

        return row_count, inconsistent_rows, min_cols, max_cols

    @step
    def process_csv(self):
        """Process the CSV file and extract information about rows and columns.

        Includes error handling for various failure scenarios.
        """
        try:
            # Convert bytes to string if needed
            csv_content = self.csv_file
            if isinstance(csv_content, bytes):
                csv_content = csv_content.decode("utf-8")

            # Validate CSV content
            self._validate_csv_content(csv_content)

            # Parse the CSV
            reader = csv.reader(StringIO(csv_content))

            # Read the first row to get column names
            try:
                header = next(reader)

                # Process rows and check consistency
                row_count, inconsistent_rows, min_cols, max_cols = (
                    self._check_rows_consistency(reader, header)
                )

                self.row_count = row_count
                self.inconsistent_rows = inconsistent_rows
                self.min_cols = min_cols
                self.max_cols = max_cols

                # Print information about the CSV
                print("Successfully processed CSV file")
                print(f"Number of columns in header: {self.column_count}")
                print(f"Column names: {self.column_names}")
                print(f"Number of data rows: {self.row_count}")

                # Report on data quality issues
                if inconsistent_rows > 0:
                    print(
                        f"Warning: {inconsistent_rows} rows have inconsistent "
                        f"column counts"
                    )
                    print(f"Column count range: {min_cols} to {max_cols}")
                    self.data_quality_issues = True
                else:
                    print("All rows have consistent column counts")
                    self.data_quality_issues = False

            except StopIteration as err:
                error_msg = "CSV file contains a header but no data rows"
                raise ValueError(error_msg) from err

        except csv.Error as e:
            print(f"CSV parsing error: {e!s}")
            self.error_message = f"Failed to parse CSV: {e!s}"
        except ValueError as e:
            print(f"Value error: {e!s}")
            self.error_message = str(e)
        except Exception as e:
            print(f"Unexpected error: {e!s}")
            self.error_message = f"Unexpected error processing CSV: {e!s}"
        else:
            # If no errors, proceed
            self.error_message = None

        # Always proceed to the end step
        self.next(self.end)

    @step
    def end(self):
        """End of the flow."""
        if hasattr(self, "error_message") and self.error_message:
            print(f"Flow completed with errors: {self.error_message}")
        else:
            print(
                f"Flow completed successfully. Processed CSV with "
                f"{self.row_count} rows and {self.column_count} columns."
            )
            if hasattr(self, "data_quality_issues") and self.data_quality_issues:
                print(
                    f"Note: Data quality issues detected - "
                    f"{self.inconsistent_rows} rows with inconsistent column counts"
                )


if __name__ == "__main__":
    CSVProcessingFlow()


# How to run this flow with different CSV files:
#
# 1. Run with the default sample CSV file (well-formed data):
#    uv run a8.py run
#
# 2. Run with an empty CSV file to test error handling:
#    uv run a8.py run --csv_file ../../data/empty.csv
#
# 3. Run with a malformed CSV file:
#    uv run a8.py run --csv_file ../../data/malformed.csv
#
# 4. Run with a severely malformed CSV file:
#    uv run a8.py run --csv_file ../../data/very_malformed.csv
#
# 5. Run with a CSV file that has inconsistent column counts:
#    uv run a8.py run --csv_file ../../data/inconsistent.csv
#
# Expected behaviors:
# - Empty CSV: Flow will error with "The CSV file is empty"
# - Malformed CSV: The CSV parser may handle some malformed syntax
# - Inconsistent columns: Flow will detect and report rows with
#   inconsistent column counts
