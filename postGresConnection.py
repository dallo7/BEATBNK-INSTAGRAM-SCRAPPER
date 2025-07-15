import psycopg2
import psycopg2.sql as sql


class Database:
    """
    A class to manage a PostgreSQL database connection using a context manager.
    """

    def __init__(self, db_params):
        """
        Initializes the Database object with connection parameters.

        Args:
            db_params (dict): A dictionary containing the database connection parameters.
        """
        self.db_params = db_params
        self.connection = None
        self.cursor = None

    def __enter__(self):
        """
        Establishes the database connection when entering the 'with' block.

        Returns:
            Database: The instance of the Database class.
        """
        try:
            print("Connecting to the PostgreSQL database...")
            self.connection = psycopg2.connect(**self.db_params)
            print("Connection successful.")
            return self
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error while connecting to PostgreSQL: {error}")
            # Reraise the exception to prevent the 'with' block from executing
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the cursor and connection when exiting the 'with' block.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def select_all(self, table_name):
        """
        Selects all records from a specified table.

        Args:
            table_name (str): The name of the table to query.

        Returns:
            list: A list of tuples, where each tuple represents a row from the table.
        """
        records = []
        try:
            self.cursor = self.connection.cursor()

            # Use psycopg2.sql to safely quote the table name
            query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

            print(f"Selecting all from the {table_name} table...")
            self.cursor.execute(query)

            records = self.cursor.fetchall()
            print(f"Found {len(records)} records in {table_name}.")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error during query execution for table {table_name}: {error}")

        return records

    def select_from_multiple_tables(self, table_names):
        """
        Selects all records from a list of specified tables.

        Args:
            table_names (list): A list of table names to query.

        Returns:
            dict: A dictionary where keys are table names and values are lists of records.
        """
        all_data = {}
        for table in table_names:
            all_data[table] = self.select_all(table)
        return all_data


if __name__ == '__main__':
    db_connection_params = {
        "dbname": "beatbnk_db",
        "user": "user",
        "password": "X1SOrzeSrk",
        "host": "beatbnk-db.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com",
        "port": "5432"
    }

    # List of tables identified from the user's images
    tables_to_query = [
        "SequelizeMeta", "attendees", "categories", "category_mappings",
        "event_tickets", "events", "follows", "genres", "group_permissions",
        "groups", "interests", "media_files", "media_types",
        "mpesa_stk_push_payments", "otps", "performer_genres",
        "performer_tip_payments", "performer_tips", "performers",
        "permissions", "refresh_tokens", "son g_request_payments",
        "song_requests", "tickets", "user_fcm_tokens", "user_groups",
        "user_interests", "user_venue_bookings", "users", "venue_bookings",
        "venues"
    ]

    try:
        # Use the Database class as a context manager
        with Database(db_connection_params) as db:
            # Call the method to get data from all specified tables
            all_table_data = db.select_from_multiple_tables(tables_to_query)

            # Print the results for each table
            for table_name, records in all_table_data.items():
                print(f"\n--- Records from {table_name} ---")
                if records:
                    for row in records:
                        print(row)
                else:
                    print("No records found.")
                print("-----------------------------------\n")

    except Exception as e:
        # This will catch connection errors raised from __enter__
        print(f"Failed to execute database operations. Reason: {e}")

