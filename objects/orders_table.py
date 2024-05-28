import pandas as pd
import os


class OrdersTable:
    def __init__(self, strategy=None):
        """
        Initialize the OrdersTable object.

        Args:
        - strategy: Optional strategy object used for formatting data.

        """
        # Initialize DataFrame to store orders
        self.orders_df = pd.DataFrame()

        # Set the strategy for data formatting
        self.strategy = strategy
        

    def save_row(self, row):
        """
        Save a row of data to the orders DataFrame.

        Args:
        - row: Dictionary representing the order data.

        """
        # ? esto no te esta andando porque inicializas el objeto de orderstable sin estrategia primero y luego le sumas la estrategia pero no siente el impacto
        # Format the row using the strategy, if provided
        if self.strategy:
            row = self.strategy.format_df_of_orders(row)

        # Convert row to DataFrame and append to orders DataFrame
        row_df = pd.DataFrame([row])
        self.orders_df = pd.concat([self.orders_df, row_df], ignore_index=True)

    
    def create_excel(self):
        """
        Create an Excel file with the orders data.

        """
        # Define the name for the Excel file
        excel_name = "operaciones.xlsx"

        # Remove existing Excel file if it exists
        if os.path.exists(excel_name):
            os.remove(excel_name)

        # Write orders DataFrame to Excel file
        self.orders_df.to_excel(excel_name, index=False)

        # Print success message
        print(f"Excel '{excel_name}' creado exitosamente :)")


