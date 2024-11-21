import happybase 
import pandas as pd 
from datetime import datetime 
 
# Bloque principal de ejecución 
try: 
    # 1. Establecer conexión con HBase     
    connection = happybase.Connection('localhost') 
    print("Conexión establecida con HBase") 
 
    # 2. Crear la tabla con las familias de columnas     
    table_name = 'used_cars'     
    families = { 
        'basic': dict(),        # información básica del coche 
        'specs': dict(),        # especificaciones técnicas 
        'sales': dict(),        # información de venta 
        'condition': dict()     # estado del vehículo 
    } 
 
    # Eliminar la tabla si ya existe     
    if table_name.encode() in connection.tables(): 
        print(f"Eliminando tabla existente - {table_name}") 
        connection.delete_table(table_name, disable=True) 
 
    # Crear nueva tabla 
    connection.create_table(table_name, families)     
    table = connection.table(table_name)     
    print("Tabla 'used_cars' creada exitosamente") 
 
    # 3. Cargar datos del CSV 
    car_data = pd.read_csv('Car_details_v3.csv') 
     
    # Iterar sobre el DataFrame usando el índice     
    for index, row in car_data.iterrows():         
        # Generar row key basado en el índice 
        row_key = f'car_{index}'.encode() 
         
        # Organizar los datos en familias de columnas         
        data = { 
            b'basic:name': str(row['name']).encode(),             
            b'basic:year': str(row['year']).encode(),             
            b'basic:transmission': str(row['transmission']).encode(),             
            b'basic:fuel': str(row['fuel']).encode(), 
             
            b'specs:engine': str(row['engine']).encode(),             
            b'specs:max_power': str(row['max_power']).encode(),             
            b'specs:torque': str(row['torque']).encode(),             
            b'specs:seats': str(row['seats']).encode(), 
            b'specs:mileage': str(row['mileage']).encode(), 
             
            b'sales:selling_price': str(row['selling_price']).encode(), 
            b'sales:seller_type': str(row['seller_type']).encode(), 
             
            b'condition:km_driven': str(row['km_driven']).encode(), 
            b'condition:owner': str(row['owner']).encode() 
        } 
         
        table.put(row_key, data) 
     
    print("Datos cargados exitosamente") 
 
except Exception as e: 
    print(f"Error: {str(e)}") 
finally: 
    # Cerrar la conexión 
    connection.close() 
