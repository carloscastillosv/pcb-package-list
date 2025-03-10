import os
import xlrd
import pandas as pd
import configparser
import uuid
import sqlite3
from datetime import datetime

def get_po_excel_files():
    folder_path = input("Enter the folder path: ").strip()
    
    if not os.path.isdir(folder_path):
        print("Invalid folder path. Please enter a valid directory.")
        return

    common_prefix = "PO"
    po_files = [f for f in os.listdir(folder_path) if f.startswith(common_prefix) and f.lower().endswith((".xls", "xlsx", "xlsm"))]
    
    if not po_files:
        print("No Excel files starting with 'PO' found.")
        return
    
    processguid = str(uuid.uuid4())  # Single GUID for all files processed
    all_parent_data = []
    all_detail_data = []
    
    conn = sqlite3.connect("po_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS process_control (
            ProcessGUID TEXT,
            FileGUID TEXT,
            FileName TEXT,
            StartProcess DATETIME,
            EndProcess DATETIME
        )
    """)
    conn.commit()
        
    for file in po_files:
        print(f"Processing: {file}")
        fileguid = str(uuid.uuid4())
        file_start_time = datetime.now()
        parent_data, detail_data = process_excel_file(os.path.join(folder_path, file), processguid, fileguid)
        file_end_time = datetime.now()
        
        if parent_data and detail_data:
            all_parent_data.extend(parent_data)
            all_detail_data.extend(detail_data)
        
        cursor.execute("""
            INSERT INTO process_control (ProcessGUID, FileGUID, FileName, StartProcess, EndProcess) 
            VALUES (?, ?, ?, ?, ?)
        """, (processguid, fileguid, file, file_start_time, file_end_time))
        conn.commit()
    
    if all_parent_data and all_detail_data:
        df_parent = pd.DataFrame(all_parent_data)
        df_child = pd.DataFrame(all_detail_data)
        
        df_parent.to_sql("informacion_comercial", conn, if_exists="append", index=False)
        df_child.to_sql("informacion_variable", conn, if_exists="append", index=False)
        
        query = """
            SELECT ic.Process_GUID as PROCESSGUID,
		ic.File_GUID  as FILEGUID,
            ic.ordencompra as OC, 
            ic.pedido as PEDIDO, 
            iv.size as TALLA, 
            iv.upc as UPC, 
            iv.composicion as COMPOSICION, 
            " " as Material,
            ic.descripcion as DESCRIPCION,
            iv.qty*1000 as CANTIDAD,
            CEIL(iv.qty*1000/ic.observaciones) as CANTIDAD_CHAROLAS,
            REPLACE(CAST(FLOOR(iv.qty*1000/ic.observaciones) AS TEXT) || '(' || CAST(FLOOR(iv.qty*1000/ic.observaciones)*ic.observaciones AS TEXT) || ') 1.0(' || CAST(iv.qty*1000%ic.observaciones AS TEXT) || ')','.0','') as DETALLE            
        FROM informacion_comercial ic
        INNER JOIN informacion_variable iv 
        ON (ic.Process_GUID  = iv.Process_GUID 
		and ic.File_GUID  = iv.File_GUID)
        where ic.Process_GUID = ?
        """
        df_joined = pd.read_sql(query, conn, params=[processguid])
        
        output_file = os.path.join(folder_path, "joined_data.xlsx")
        df_joined.to_excel(output_file, sheet_name="Joined_Data", index=False)
        print(f"Excel file '{output_file}' created successfully with joined data.")
    
    conn.close()
    print("Data successfully saved to SQLite database.")

def process_excel_file(file_path, processguid, fileguid):
    config = configparser.ConfigParser()
    config.read("config.ini")
    
    workbook = xlrd.open_workbook(file_path)
    
    sheet = workbook.sheet_by_name("Información Comercial")
    parent_data = []
    
    parent_entry = {"Process_GUID": processguid, "File_GUID": fileguid}

    for key, value in config["MainTable"].items():
        row, col = map(int, value.split(","))
        parent_entry[key] = sheet.cell_value(row, col)
    
    parent_data.append(parent_entry)
    
    sheet_child = workbook.sheet_by_name("Información variable")
    detail_data = []
    
    for row_idx in range(2, sheet_child.nrows):
        detail_entry = {"Process_GUID": processguid, "File_GUID": fileguid}
        for key, value in config["DetailTable"].items():
            col = int(value)
            detail_entry[key] = sheet_child.cell_value(row_idx, col)
        detail_data.append(detail_entry)
    
    return parent_data, detail_data

if __name__ == "__main__":
    get_po_excel_files()
