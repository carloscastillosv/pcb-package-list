import os
import xlrd
import pandas as pd
import configparser
import uuid
import sqlite3
from datetime import datetime
from sqlite3 import register_adapter

# Register a custom adapter for datetime
def adapt_datetime(dt):
    return dt.isoformat()

register_adapter(datetime, adapt_datetime)


def get_po_excel_files():
    """
    Get all Excel files starting with 'PO' in a folder and process them.
    The processed data is saved to a SQLite database and a joined Excel file is created.

    param: None
    return: None
    """
    folder_path = input("Enter the folder path: ").strip()

    if not os.path.isdir(folder_path):
        print("Invalid folder path. Please enter a valid directory.")
        return

    common_prefix = "PO"

    po_files = [
        f for f in os.listdir(folder_path)
        if f.startswith(common_prefix) and f.lower().endswith((".xls", "xlsx", "xlsm"))
    ]

    if not po_files:
        print("No Excel files starting with 'PO' found.")
        return

    processguid = str(uuid.uuid4())  # Single GUID for all files processed

    all_ic_data = []
    all_iv_data = []

    # Connect to SQLite database
    conn = sqlite3.connect("po_data.db", detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS process_control (
            process_guid TEXT,
            file_guid TEXT,
            file_name TEXT,
            start_datetime DATETIME,
            end_datetime DATETIME
        )
    """)
    conn.commit()

    for file in po_files:
        print(f"Processing: {file}")
        fileguid = str(uuid.uuid4())
        file_start_time = datetime.now()
        ic_data, iv_data = process_excel_file(
            os.path.join(folder_path, file), processguid, fileguid)
        file_end_time = datetime.now()

        if ic_data and iv_data:
            all_ic_data.extend(ic_data)
            all_iv_data.extend(iv_data)

        cursor.execute("""
            INSERT INTO process_control (process_guid, file_guid, file_name, start_datetime, end_datetime) 
            VALUES (?, ?, ?, ?, ?)
        """, (processguid, fileguid, file, file_start_time, file_end_time))
        conn.commit()

    if all_ic_data and all_iv_data:
        df_parent = pd.DataFrame(all_ic_data)
        df_child = pd.DataFrame(all_iv_data)

        # Trim spaces in string fields
        df_parent = df_parent.map(lambda x: x.strip() if isinstance(x, str) else x)
        df_child = df_child.map(lambda x: x.strip() if isinstance(x, str) else x)

        df_parent.to_sql("informacion_comercial", conn, if_exists="append", index=False)
        df_child.to_sql("informacion_variable", conn, if_exists="append", index=False)

        query = """
            SELECT 
                ic.orden_compra as OC, 
                ic.pedido as PEDIDO, 
                iv.talla as TALLA, 
                iv.upc as UPC, 
                iv.composicion as COMPOSICION, 
                " " as Material,
                ic.descripcion as DESCRIPCION,
                iv.qty*1000 as CANTIDAD,
                CEIL(iv.qty*1000/ic.observaciones) as CANTIDAD_CHAROLAS,
                CASE WHEN FLOOR(iv.qty*1000/ic.observaciones) > 0 THEN
                    REPLACE(CAST(FLOOR(iv.qty*1000/ic.observaciones) AS TEXT) || '(' || CAST(FLOOR(iv.qty*1000/ic.observaciones)*ic.observaciones AS TEXT) || ') 1.0(' || CAST(iv.qty*1000%ic.observaciones AS TEXT) || ')','.0','') 
                ELSE
                    REPLACE('1 (' || CAST(iv.qty*1000%ic.observaciones AS TEXT) || ')','.0','') 
                END
                AS DETALLE
            FROM informacion_comercial ic
            INNER JOIN informacion_variable iv 
            ON (ic.process_guid  = iv.process_guid AND ic.file_guid  = iv.file_guid)
            WHERE ic.process_guid = ?
        """
        df_joined = pd.read_sql(query, conn, params=[processguid])

        # Trim spaces in string fields for the joined DataFrame
        df_joined = df_joined.map(lambda x: x.strip() if isinstance(x, str) else x)

        config = configparser.ConfigParser()
        config.read("config.ini")
        date_format = config["General"]["dateformat"]

        date_str = datetime.now().strftime(date_format)

        output_file = os.path.join(f"Lista de Empaque - {date_str}.xlsx")

        df_joined.to_excel(output_file, sheet_name="Joined_Data", index=False)
        print(f"Excel file '{output_file}' created successfully with joined data.")

    conn.close()
    print("Data successfully saved to SQLite database.")
    input("Press any key to close")


def process_excel_file(file_path, processguid, fileguid):
    """
    Process the Excel file and return the data in a structured format.

    Args:
        file_path (str): Path of the Excel file to be processed
        processguid (str): GUID for the process
        fileguid (str): GUID for the file

    Returns:
        tuple: A tuple containing the parent data and detail data
    """
    config = configparser.ConfigParser()
    config.read("config.ini")

    workbook = xlrd.open_workbook(file_path)

    # Process "Información Comercial" sheet
    sheet = workbook.sheet_by_name("Información Comercial")
    ic_data = []

    ic_entry = {"process_guid": processguid, "file_guid": fileguid}

    for key, value in config["MainTable"].items():
        row, col = map(int, value.split(","))
        ic_entry[key] = sheet.cell_value(row, col)

    ic_data.append(ic_entry)

    # Process "Información variable" sheet
    sheet_child = workbook.sheet_by_name("Información variable")
    iv_data = []

    for row_idx in range(2, sheet_child.nrows):
        iv_entry = {"process_guid": processguid, "file_guid": fileguid}
        for key, value in config["DetailTable"].items():
            col = int(value)
            iv_entry[key] = sheet_child.cell_value(row_idx, col)
        iv_data.append(iv_entry)

    return ic_data, iv_data


if __name__ == "__main__":
    get_po_excel_files()
