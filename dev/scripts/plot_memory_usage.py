#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import sys

def plot_memory_usage(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Create time index based on row order (each row is a sequential time point)
    df['time'] = range(len(df))

    # Pivot the data to have consumers as columns
    pivot_df = df.pivot(index='time', columns='name', values='size')
    pivot_df = pivot_df.fillna(method='ffill').fillna(0)

    # Create stacked area chart
    plt.figure(figsize=(8, 4))
    plt.stackplot(pivot_df.index,
                  [pivot_df[col] for col in pivot_df.columns],
                  labels=pivot_df.columns,
                  alpha=0.8)

    # Add annotations for ERR labels
    if 'label' in df.columns:
        err_points = df[df['label'].str.contains('ERR', na=False)]
        for _, row in err_points.iterrows():
            plt.axvline(x=row['time'], color='red', linestyle='--', alpha=0.7, linewidth=1.5)
            plt.text(row['time'], plt.ylim()[1] * 0.95, 'ERR',
                    ha='center', va='top', color='red', fontweight='bold')

    plt.xlabel('Time')
    plt.ylabel('Memory Usage')
    plt.title('Memory Usage Over Time by Consumer')
    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save the plot
    output_file = csv_file.replace('.csv', '_chart.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Chart saved to: {output_file}")
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python plot_memory_usage.py <csv_file>")
        sys.exit(1)

    plot_memory_usage(sys.argv[1])
