import sqlite3
import pandas as pd
import os
from pathlib import Path


def create_database(db_path: str, data_dir: str):    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Creating database at: {db_path}")
    print(f"Reading CSV files from: {data_dir}")
    
    datasets = {
        'customers': 'olist_customers_dataset.csv',
        'geolocation': 'olist_geolocation_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv',
        'order_payments': 'olist_order_payments_dataset.csv',
        'order_reviews': 'olist_order_reviews_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'product_category_translation': 'product_category_name_translation.csv'
    }
    
    # Process each dataset
    for table_name, csv_file in datasets.items():
        csv_path = os.path.join(data_dir, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"Warning: {csv_file} not found, skipping...")
            continue
        
        print(f"\nProcessing {csv_file}...")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        print(f"  - Rows: {len(df)}")
        print(f"  - Columns: {list(df.columns)}")
        
        # Write to SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"  ! Created table '{table_name}'")
    
    # Create indexes for better query performance
    print("\nCreating indexes...")
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_customers_id ON customers(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_customers_unique_id ON customers(customer_unique_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_id ON orders(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_items_seller_id ON order_items(seller_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_payments_order_id ON order_payments(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_reviews_order_id ON order_reviews(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_id ON products(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_sellers_id ON sellers(seller_id)",
        "CREATE INDEX IF NOT EXISTS idx_geolocation_zip ON geolocation(geolocation_zip_code_prefix)"
    ]
    
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            print(f"  ✓ {index_sql.split('idx_')[1].split(' ON')[0]}")
        except Exception as e:
            print(f"  ✗ Error creating index: {e}")
    
    # Commit changes and show summary
    conn.commit()
    
    print("\n" + "="*60)
    print("DATABASE SUMMARY")
    print("="*60)
    
    # Get table information
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  {table_name}: {count:,} rows")
    
    # Close connection
    conn.close()
    
    print("\n! Database created successfully!")
    return db_path


def main():
    """Main function to create the Olist database."""
    # Set paths
    script_dir = Path(__file__).parent
    data_dir = script_dir / "olist_data"
    db_path = script_dir / "olist.db"
    
    # Create database
    create_database(str(db_path), str(data_dir))
    
    print(f"\nDatabase location: {db_path}")


if __name__ == "__main__":
    main()