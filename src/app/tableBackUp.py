import oracledb
from src.config.appConfig import getJsonConfig
import pandas as pd
import os
from datetime import datetime

class CodeBookBackUp():
    """This class fetches iegc violation messages for UI
    """
    def __init__(self, dbConStr: str):
        """constructor method
        Args:
            con_string ([str]): connection string
        """

        self.localConStr = dbConStr

    def codeBookTableBackUp(self, table_name, output_dir):
        dbConfig = getJsonConfig()
        output_dir = dbConfig['output_dir']
        dbConn = None
        cursor = None
        try:
            dbConn = oracledb.connect(self.localConStr)

            # get cursor for raw data table
            cursor = dbConn.cursor()

            # Construct query to select all data from the table
            query = f"SELECT * FROM CODE_BOOK.OP_CODES"
            
            # Execute the query
            cursor.execute(query)
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=column_names)
            
            # Determine output directory
            if output_dir is None:
                output_dir = os.path.join(os.getcwd(), 'database_backups')
            
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%d_%m_%Y")
            filename = f"{table_name}_backup_{timestamp}.xlsx"
            full_path = os.path.join(output_dir, filename)
            
            # Export to Excel
            df.applymap(lambda x: x.encode('unicode_escape').
                 decode('utf-8') if isinstance(x, str) else x).to_excel(full_path, index=False)
            
            print(f"Backup successful! File saved to: {full_path}")
            print(f"Total rows backed up: {len(df)}")
            
            # # Close cursor and connection
            # return full_path
        
        except oracledb.Error as error:
            print(f"Error during database backup: {error}")
            return None
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            return None

        finally:
            # closing database cursor and connection
            if cursor is not None:
                cursor.close()
            dbConn.close()
            print('closed db connection after backing up of OP_CODES table')