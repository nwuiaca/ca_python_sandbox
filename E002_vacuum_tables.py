import sqlite3

def delete_tables_except_keep_list(db_path, keep_list):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("VACUUM;")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Specify the tables that you want to keep
keep_list = ['X000_Own_kfs_lookups', 'X000_Vendor', 'X001aa_Report_payments', 'X000_PEOPLE_CONTACT']

# Call the function to delete tables except the ones in the keep list
db_path = 'W:\Kfs\Kfs.sqlite'
delete_tables_except_keep_list(db_path, keep_list)
