#!/usr/bin/env python3
"""
Large CSV Generator for DataPy Demo

Generates a 2GB CSV file with realistic customer data for testing
both pandas and polars pipeline performance.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
from datetime import datetime, timedelta

# Data generation parameters
TARGET_SIZE_GB = 2.0
ESTIMATED_BYTES_PER_ROW = 180  # Approximate bytes per row based on columns
TARGET_ROWS = int((TARGET_SIZE_GB * 1024 * 1024 * 1024) / ESTIMATED_BYTES_PER_ROW)

def generate_customer_data(num_rows: int) -> pd.DataFrame:
    """
    Generate realistic customer data with multiple columns.
    
    Filter conditions will target:
    - Age 25-65 (adult working population) 
    - Annual income >= $30,000
    - Account balance >= $1,000
    - Active customers (last purchase within 2 years)
    """
    print(f"Generating {num_rows:,} customer records...")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Generate customer IDs
    customer_ids = [f"CUST{i:08d}" for i in range(1, num_rows + 1)]
    
    # Generate names
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Jennifer", 
                   "William", "Amanda", "James", "Jessica", "Christopher", "Ashley", "Daniel", "Emily"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"]
    
    names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(num_rows)]
    
    # Generate demographics - mixed distribution for filtering
    # 60% will be in target age range (25-65), 40% outside
    ages = []
    for _ in range(num_rows):
        if random.random() < 0.6:
            ages.append(random.randint(25, 65))  # Target range
        else:
            ages.append(random.choice([random.randint(18, 24), random.randint(66, 85)]))  # Outside range
    
    genders = np.random.choice(["M", "F", "O"], num_rows, p=[0.48, 0.48, 0.04])
    
    # Generate locations
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", 
              "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
              "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX", "FL",
              "TX", "OH", "NC", "CA", "IN", "WA"]
    city_state_pairs = list(zip(cities, states))
    
    customer_cities = []
    customer_states = []
    for _ in range(num_rows):
        city, state = random.choice(city_state_pairs)
        customer_cities.append(city)
        customer_states.append(state)
    
    # Generate financial data - mixed for filtering
    # 65% will meet income/balance criteria, 35% won't
    annual_incomes = []
    account_balances = []
    credit_scores = []
    
    for _ in range(num_rows):
        if random.random() < 0.65:
            # Target customers - higher income/balance
            annual_incomes.append(random.randint(30000, 150000))
            account_balances.append(random.uniform(1000, 50000))
            credit_scores.append(random.randint(650, 850))
        else:
            # Non-target customers - lower income/balance
            annual_incomes.append(random.randint(15000, 29999))
            account_balances.append(random.uniform(0, 999))
            credit_scores.append(random.randint(300, 649))
    
    # Generate dates
    registration_dates = []
    last_purchase_dates = []
    base_date = datetime(2020, 1, 1)
    
    for _ in range(num_rows):
        # Registration date: 2020-2024
        reg_days = random.randint(0, 1460)  # 4 years
        reg_date = base_date + timedelta(days=reg_days)
        registration_dates.append(reg_date.strftime("%Y-%m-%d"))
        
        # Last purchase date - 70% active (within 2 years), 30% inactive
        if random.random() < 0.7:
            # Active customers - purchase within last 2 years
            purchase_days = random.randint(0, 730)
            purchase_date = datetime(2024, 1, 1) - timedelta(days=purchase_days)
        else:
            # Inactive customers - purchase more than 2 years ago
            purchase_days = random.randint(731, 1460)
            purchase_date = datetime(2024, 1, 1) - timedelta(days=purchase_days)
        
        last_purchase_dates.append(purchase_date.strftime("%Y-%m-%d"))
    
    # Generate contact info
    emails = [f"{name.lower().replace(' ', '.')}@email.com" for name in names]
    phones = [f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}" 
              for _ in range(num_rows)]
    
    # Generate product preferences
    product_categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Automotive", "Health", "Travel"]
    preferred_categories = [random.choice(product_categories) for _ in range(num_rows)]
    
    # Generate purchase metrics
    total_purchases = [random.randint(1, 50) for _ in range(num_rows)]
    avg_order_values = [round(random.uniform(25, 500), 2) for _ in range(num_rows)]
    
    # Create DataFrame
    df = pd.DataFrame({
        'customer_id': customer_ids,
        'name': names,
        'email': emails,
        'phone': phones,
        'age': ages,
        'gender': genders,
        'city': customer_cities,
        'state': customer_states,
        'annual_income': annual_incomes,
        'account_balance': account_balances,
        'credit_score': credit_scores,
        'registration_date': registration_dates,
        'last_purchase_date': last_purchase_dates,
        'preferred_category': preferred_categories,
        'total_purchases': total_purchases,
        'avg_order_value': avg_order_values
    })
    
    return df


def main():
    """Generate large CSV file for demo."""
    print("=== DataPy Demo - Large CSV Generator ===")
    print(f"Target size: {TARGET_SIZE_GB}GB")
    print(f"Estimated rows: {TARGET_ROWS:,}")
    print()
    
    # Create output directory
    output_dir = Path("projects/demo_jobs/data")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "customers_large.csv"
    
    # Generate data
    df = generate_customer_data(TARGET_ROWS)
    
    print(f"Generated DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Write to CSV
    print(f"Writing to: {output_file}")
    df.to_csv(output_file, index=False)
    
    # Check file size
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    file_size_gb = file_size_mb / 1024
    
    print(f"File created successfully!")
    print(f"File size: {file_size_mb:.1f} MB ({file_size_gb:.2f} GB)")
    print(f"Actual rows: {len(df):,}")
    
    # Show sample data
    print("\nSample data (first 5 rows):")
    print(df.head().to_string())
    
    # Show filtering preview
    print("\n=== Filter Condition Preview ===")
    filter_condition = (
        (df['age'] >= 25) & (df['age'] <= 65) &
        (df['annual_income'] >= 30000) &
        (df['account_balance'] >= 1000) &
        (pd.to_datetime(df['last_purchase_date']) >= '2022-01-01')
    )
    
    filtered_count = filter_condition.sum()
    filter_percentage = (filtered_count / len(df)) * 100
    
    print(f"Rows matching filter conditions: {filtered_count:,} ({filter_percentage:.1f}%)")
    print("Filter conditions:")
    print("  - Age: 25-65 years")
    print("  - Annual income: >= $30,000")
    print("  - Account balance: >= $1,000")
    print("  - Last purchase: within 2 years (>= 2022-01-01)")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)