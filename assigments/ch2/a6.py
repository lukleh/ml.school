"""Module for generating and visualizing random clustered data using Metaflow."""

import base64
import io

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from metaflow import FlowSpec, Parameter, card, step


class DataVisualizationFlow(FlowSpec):
    """Flow generating a dataset and visualizing it with @card decorator."""

    num_points = Parameter(
        "num_points",
        help="Number of data points to generate",
        default=100,
    )

    num_clusters = Parameter(
        "num_clusters",
        help="Number of clusters to generate",
        default=3,
    )

    @step
    def start(self):
        """Generate a random dataset with clusters."""
        # Set a seed for reproducibility
        rng = np.random.default_rng(seed=42)

        # Generate clustered data
        self.data = []
        self.cluster_labels = []

        for i in range(self.num_clusters):
            # Create a cluster center
            center_x = rng.uniform(-10, 10)
            center_y = rng.uniform(-10, 10)

            # Generate points around this center
            points_per_cluster = self.num_points // self.num_clusters

            cluster_x = rng.normal(center_x, 1.5, points_per_cluster)
            cluster_y = rng.normal(center_y, 1.5, points_per_cluster)

            for j in range(points_per_cluster):
                self.data.append([cluster_x[j], cluster_y[j]])
                self.cluster_labels.append(i)

        # Convert to pandas DataFrame
        self.df = pd.DataFrame(self.data, columns=["x", "y"])
        self.df["cluster"] = self.cluster_labels

        # Calculate basic statistics
        self.stats = {}
        for i in range(self.num_clusters):
            cluster_data = self.df[self.df["cluster"] == i]
            self.stats[f"Cluster {i}"] = {
                "count": len(cluster_data),
                "x_mean": cluster_data["x"].mean(),
                "y_mean": cluster_data["y"].mean(),
                "x_std": cluster_data["x"].std(),
                "y_std": cluster_data["y"].std(),
            }

        self.next(self.visualize_data)

    def _fig_to_base64(self, fig):
        """Convert a matplotlib figure to a base64 encoded string for HTML embedding."""
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        img_str = base64.b64encode(buf.read()).decode("utf-8")
        buf.close()
        return img_str

    @card(type="html")
    @step
    def visualize_data(self):
        """Visualize the generated dataset using multiple charts."""
        # Create a scatter plot
        fig1, ax1 = plt.subplots(figsize=(10, 6))
        scatter = ax1.scatter(
            self.df["x"], self.df["y"], c=self.df["cluster"],
            cmap="viridis", alpha=0.8, s=50,
        )
        ax1.set_title("Generated Clusters", fontsize=16)
        ax1.set_xlabel("X", fontsize=12)
        ax1.set_ylabel("Y", fontsize=12)
        ax1.grid(alpha=0.3)
        legend = ax1.legend(*scatter.legend_elements(), title="Clusters")
        ax1.add_artist(legend)
        plt.tight_layout()
        scatter_img = self._fig_to_base64(fig1)

        # Create histograms for x and y distributions
        fig2, (ax2, ax3) = plt.subplots(1, 2, figsize=(12, 5))

        # X distribution by cluster
        for i in range(self.num_clusters):
            cluster_data = self.df[self.df["cluster"] == i]
            ax2.hist(cluster_data["x"], alpha=0.5, bins=15, label=f"Cluster {i}")
        ax2.set_title("X Distribution by Cluster", fontsize=14)
        ax2.set_xlabel("X Value", fontsize=12)
        ax2.set_ylabel("Frequency", fontsize=12)
        ax2.legend()

        # Y distribution by cluster
        for i in range(self.num_clusters):
            cluster_data = self.df[self.df["cluster"] == i]
            ax3.hist(cluster_data["y"], alpha=0.5, bins=15, label=f"Cluster {i}")
        ax3.set_title("Y Distribution by Cluster", fontsize=14)
        ax3.set_xlabel("Y Value", fontsize=12)
        ax3.set_ylabel("Frequency", fontsize=12)
        ax3.legend()

        plt.tight_layout()
        hist_img = self._fig_to_base64(fig2)

        # Create cluster statistics table
        stats_table = (
            "<table border='1' style='border-collapse: collapse; width: 100%;'>"
        )
        stats_table += (
            "<tr><th>Cluster</th><th>Count</th><th>X Mean</th>"
            "<th>Y Mean</th><th>X Std Dev</th><th>Y Std Dev</th></tr>"
        )

        for cluster, stat in self.stats.items():
            stats_table += (
                f"<tr><td>{cluster}</td><td>{stat['count']}</td>"
                f"<td>{stat['x_mean']:.2f}</td>"
            )
            stats_table += (
                f"<td>{stat['y_mean']:.2f}</td><td>{stat['x_std']:.2f}</td>"
                f"<td>{stat['y_std']:.2f}</td></tr>"
            )

        stats_table += "</table>"

        # Generate HTML for the card
        self.html = f"""
        <h1 style="text-align: center; color: #2C3E50;">
            Random Cluster Data Visualization
        </h1>

        <div style="background-color: #f5f5f5; padding: 20px;
            border-radius: 10px; margin-bottom: 20px;">
            <h2 style="color: #3498DB;">Generated Data Parameters</h2>
            <ul>
                <li><strong>Number of Points:</strong> {self.num_points}</li>
                <li><strong>Number of Clusters:</strong> {self.num_clusters}</li>
            </ul>
        </div>

        <div style="background-color: #f5f5f5; padding: 20px;
            border-radius: 10px; margin-bottom: 20px;">
            <h2 style="color: #3498DB;">Cluster Scatter Plot</h2>
            <div style="text-align: center;">
                <img src="data:image/png;base64,{scatter_img}"
                    style="max-width: 100%; height: auto;" />
            </div>
            <p style="margin-top: 10px;">
                This scatter plot shows the distribution of data points
                colored by cluster assignment.
            </p>
        </div>

        <div style="background-color: #f5f5f5; padding: 20px;
            border-radius: 10px; margin-bottom: 20px;">
            <h2 style="color: #3498DB;">Distribution Histograms</h2>
            <div style="text-align: center;">
                <img src="data:image/png;base64,{hist_img}"
                    style="max-width: 100%; height: auto;" />
            </div>
            <p style="margin-top: 10px;">
                These histograms show the distribution of X and Y values
                for each cluster.
            </p>
        </div>

        <div style="background-color: #f5f5f5; padding: 20px; border-radius: 10px;">
            <h2 style="color: #3498DB;">Cluster Statistics</h2>
            {stats_table}
            <p style="margin-top: 10px;">
                This table shows basic statistics for each cluster.
            </p>
        </div>
        """

        self.next(self.end)

    @step
    def end(self):
        """Finish the flow execution."""
        print(
            f"Flow completed successfully with {self.num_points} data points "
            f"in {self.num_clusters} clusters"
        )
        print(
            "Run 'uv run -- python assigments/ch2/a6.py card view visualize_data' "
            "to see the visualization card"
        )


if __name__ == "__main__":
    DataVisualizationFlow()
