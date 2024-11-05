import sqlite3

def delete_tables_except_keep_list(db_path, keep_list):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Retrieve the list of all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Iterate through the list of tables
        for table_name in tables:
            # Extract the table name from the tuple
            table_name = table_name[0]
            # Check if the table is in the keep list
            if table_name not in keep_list:
                # It's not in the keep list, so we drop the table
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
                print(f"Table '{table_name}' has been deleted.")

        # Commit the transaction
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        # Close the cursor and connection
        cursor.execute("VACUUM;")
        cursor.close()
        conn.close()

# Specify the tables that you want to keep
keep_list = ['X000_Own_kfs_lookups', 'X000_Vendor', 'X001aa_Report_payments', 'X000_PEOPLE_CONTACT']

# Call the function to delete tables except the ones in the keep list
db_path = 'W:\Surtech_project\Kfs.sqlite'
delete_tables_except_keep_list(db_path, keep_list)
db_path = 'W:\Surtech_project\Kfs_curr.sqlite'
delete_tables_except_keep_list(db_path, keep_list)
db_path = 'W:\Surtech_project\Kfs_prev.sqlite'
delete_tables_except_keep_list(db_path, keep_list)
db_path = 'W:\Surtech_project\People.sqlite'
delete_tables_except_keep_list(db_path, keep_list)
