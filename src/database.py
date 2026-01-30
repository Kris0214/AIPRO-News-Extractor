"""
Database Operations Module
Handles connection and data retrieval from Oracle ODS database
"""
import time
import logging
import pandas as pd
import oracledb
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Oracle Database Manager"""
    
    def __init__(self, account: str, password: str, host: str, port: str, 
                 service_name: str, oracle_client_path: str) -> None:
        """
        Initialize database manager.
        
        Args:
            account: Database account
            password: Database password
            host: Host address
            port: Port number
            service_name: Service name
            oracle_client_path: Oracle Client path
        """
        self.account = account
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.oracle_client_path = oracle_client_path
        
        # Initialize Oracle Client
        try:
            oracledb.init_oracle_client(lib_dir=oracle_client_path)
            logger.info(f"Oracle Client initialized successfully")
        except Exception as e:
            logger.warning(f"Oracle Client initialization warning: {e}")
            logger.warning(f"Check path: {oracle_client_path}")
    
    @contextmanager
    def get_connection(self):
        """
        Get database connection as context manager.
        
        Yields:
            Database connection object
        """
        dsn = f"{self.host}:{self.port}/{self.service_name}"
        conn = None
        try:
            conn = oracledb.connect(
                user=self.account,
                password=self.password,
                dsn=dsn
            )
            logger.debug("Database connection established successfully")
            yield conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            logger.error(f"DSN: {dsn}")
            logger.error(f"Account: {self.account}")
            logger.error(f"Oracle Client: {self.oracle_client_path}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed")
    
    
    def fetch_dataframe(self, query: str, process_clob: bool = True) -> pd.DataFrame:
        """
        Execute SQL query and return DataFrame (generic method).
        
        Args:
            query: SQL query statement
            process_clob: Auto-read CLOB field content (default True)
            
        Returns:
            Query result as DataFrame
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Get data
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning("Query result is empty")
                    return pd.DataFrame(columns=columns)
                
                # Process data (support CLOB)
                if process_clob:
                    processed_rows = []
                    for row in rows:
                        processed_row = []
                        for cell in row:
                            if hasattr(cell, 'read'):
                                processed_row.append(cell.read())
                            else:
                                processed_row.append(cell)
                        processed_rows.append(processed_row)
                    data = processed_rows
                else:
                    data = rows
                
                # Create DataFrame
                df = pd.DataFrame(data, columns=columns)
                
                elapsed_time = time.time() - start_time
                logger.info(f"Query completed, {len(df)} rows, elapsed time {elapsed_time:.2f}s")
                
                return df
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise