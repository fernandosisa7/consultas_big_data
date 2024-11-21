import happybase
import pandas as pd
from datetime import datetime

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla con las familias de columnas
    table_name = 'personas'
    families = {
        'datos_personales': dict(),        # Información personal
        'datos_ubicacion': dict(),         # Ubicación de nacimiento y residencia
        'datos_academicos': dict(),        # Información académica
        'datos_sociedad': dict(),          # Sector, estrato y económico
        'datos_personales_adicionales': dict()  # Discapacidad, aliado, etc.
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'personas' creada exitosamente")

    # 3. Cargar datos desde el CSV 'g4cd-bvpd.csv'
    file_path = 'g4cd-bvpd.csv'
    persona_data = pd.read_csv(file_path)

    # Iterar sobre el DataFrame usando el índice
    for index, row in persona_data.iterrows():
        # Generar row key basado en el índice (podría ser algo más único en la práctica)
        row_key = f'persona_{index}'.encode()

        # Organizar los datos en familias de columnas
        data = {
            b'datos_personales:tipo_de_documento': str(row['tipo_de_documento']).encode(),
            b'datos_personales:nombres': str(row['nombres']).encode(),
            b'datos_personales:apellidos': str(row['apellidos']).encode(),
            b'datos_personales:fecha_de_nacimiento': str(row['fecha_de_nacimiento']).encode(),
            b'datos_personales:edad': str(row['edad']).encode(),
            b'datos_personales:genero': str(row['genero']).encode(),
            b'datos_personales:discapacidad': str(row['discapacidad']).encode(),
            b'datos_personales:aliado': str(row['aliado']).encode(),

            b'datos_ubicacion:departamento_nacimiento': str(row['departamento_nacimiento']).encode(),
            b'datos_ubicacion:ciudad_de_nacimiento': str(row['ciudad_de_nacimiento']).encode(),
            b'datos_ubicacion:departame_nombre': str(row['departame_nombre']).encode(),
            b'datos_ubicacion:municipio_nombre': str(row['municipio_nombre']).encode(),

            b'datos_academicos:nivel_educativo': str(row['nivel_educativo']).encode(),

            b'datos_sociedad:sector': str(row['sector']).encode(),
            b'datos_sociedad:estrato': str(row['estrato']).encode(),
            b'datos_sociedad:sector_economico': str(row['sector_economico']).encode(),

            b'datos_personales_adicionales:ocupacion': str(row['ocupacion']).encode(),
            b'datos_personales_adicionales:comunidad_etnica': str(row['comunidad_etnica']).encode(),
        }

        # Insertar los datos en HBase
        table.put(row_key, data)

    print("Datos cargados exitosamente")

    # 4. Consultas
    
    # a. Consulta de selección: Obtener todos los datos de una persona con un row_key específico
    row_key_to_query = b'persona_10'  # La persona con el índice 10
    result = table.row(row_key_to_query)

    # Mostrar los datos obtenidos
    print("\nDatos de la persona consultada:")
    for column, value in result.items():
        print(f"{column.decode()}: {value.decode()}")
    
    # b. Consulta de filtrado: Obtener todas las personas que pertenecen a un sector económico específico
    sector_economico_to_filter = 'Gobierno'  # Por ejemplo, sector económico 'Gobierno'
    filtered_results = []

    for key, data in table.scan():  # Escanear toda la tabla
        if data.get(b'datos_sociedad:sector_economico') == sector_economico_to_filter.encode():
            filtered_results.append(data)

    # Mostrar los resultados filtrados
    print(f"\nPersonas en el sector económico '{sector_economico_to_filter}':")
    for result in filtered_results:
        print(result)
    
    # c. Recorrido de los datos: Imprimir todos los registros de la tabla
    print("\nRecorrido de todos los registros de la tabla:")
    for key, data in table.scan():
        print(f"Row Key: {key.decode()}")
        for column, value in data.items():
            print(f"{column.decode()}: {value.decode()}")
        print("-" * 40)
    
    # d. Consulta de actualización: Actualizar la edad de una persona
    row_key_to_update = b'persona_10'  # Persona con el índice 10
    new_age = 35  # Nueva edad

    # Actualizar la edad en la familia de columnas 'datos_personales'
    table.put(row_key_to_update, {
        b'datos_personales:edad': str(new_age).encode()
    })

    print(f"\nEdad de la persona {row_key_to_update.decode()} actualizada a {new_age}")
    
    # Consulta de eliminación: Eliminar una persona específica de la tabla
    row_key_to_delete = b'persona_10'  # Persona con el índice 10

    # Eliminar la fila con el row_key especificado
    table.delete(row_key_to_delete)

    print(f"\nPersona {row_key_to_delete.decode()} eliminada exitosamente.")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()
