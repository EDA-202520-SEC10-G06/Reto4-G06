import sys
from tabulate import tabulate # type: ignore
import App.logic as lg
import os

def new_logic():
    """
        Se crea una instancia del controlador (catálogo de datos)
    """
    return lg.new_logic()


def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Vista: carga los datos eligiendo uno de los datasets disponibles.
    No pide el nombre del archivo, muestra un menú de selección.
    """

    print("""
=== SELECCIONE EL DATASET A CARGAR ===

1 - 1000_cranes_mongolia_small.csv
2 - 1000_cranes_mongolia_30pct.csv
3 - 1000_cranes_mongolia_80pct.csv
4 - 1000_cranes_mongolia_large.csv
""")

    opcion = input("Ingrese la opción deseada: ")

    if opcion == "1":
        file = "1000_cranes_mongolia_small.csv"
    elif opcion == "2":
        file = "1000_cranes_mongolia_30pct.csv"
    elif opcion == "3":
        file = "1000_cranes_mongolia_80pct.csv"
    elif opcion == "4":
        file = "1000_cranes_mongolia_large.csv"
    else:
        print("Opción inválida. Intente de nuevo.\n")
        return

    print(f"\nCargando dataset: {file} ...\n")

    start_time = lg.get_time()
    lg.load_data(control, file)
    end_time = lg.get_time()
    elapsed = lg.delta_time(start_time, end_time)

    print_data(control, elapsed)

def print_data(control, elapsed):
    """
    Vista: imprime el resumen de la carga de datos conforme al PDF.
    Muestra:
      - Total de grullas
      - Total de eventos
      - Total de nodos
      - Total de arcos en cada grafo
      - Primeros y últimos 5 nodos creados
    """

    info = lg.get_load_info(control)

    print("\n===== CARGA DE DATOS =====\n")
    print(f"Tiempo de carga: {elapsed:.3f} ms\n")

    print(f"Total de grullas reconocidas en el estudio: {info['total_grullas']}")
    print(f"Total de eventos cargados durante el estudio: {info['total_eventos']}")
    print(f"Total de nodos (vértices) del grafo: {info['total_nodos']}")
    print(f"Total de arcos en el grafo de movimientos: {info['total_arcos_mov']}")
    print(f"Total de arcos en el grafo de proximidad a fuentes hídricas: {info['total_arcos_agua']}\n")

    # Primeros 5 nodos
    print("--- Primeros 5 nodos creados ---")
    primeros = info.get("primeros_5", [])
    if len(primeros) == 0:
        print("No hay nodos para mostrar.")
    else:
        rows_prim = []
        i = 0
        while i < len(primeros):
            v = primeros[i]
            pos_str = f"({v['lat']}, {v['lon']})"
            fecha_str = str(v["fecha"])
            tags_str = ", ".join(v["tags"])
            rows_prim.append([
                v["id"],
                pos_str,
                fecha_str,
                tags_str,
                v["num_eventos"],
                f"{v['dist_agua_prom_km']:.4f}",
            ])
            i += 1

        headers_prim = [
            "Id vértice",
            "Posición (lat, lon)",
            "Fecha de creación",
            "Grullas (tags)",
            "Conteo de eventos",
            "Dist. prom. agua (km)",
        ]
        print(tabulate(rows_prim, headers=headers_prim, tablefmt="grid"))

    # Últimos 5 nodos
    print("\n--- Últimos 5 nodos creados ---")
    ultimos = info.get("ultimos_5", [])
    if len(ultimos) == 0:
        print("No hay nodos para mostrar.")
    else:
        rows_ult = []
        j = 0
        while j < len(ultimos):
            v = ultimos[j]
            pos_str = f"({v['lat']}, {v['lon']})"
            fecha_str = str(v["fecha"])
            tags_str = ", ".join(v["tags"])
            rows_ult.append([
                v["id"],
                pos_str,
                fecha_str,
                tags_str,
                v["num_eventos"],
                f"{v['dist_agua_prom_km']:.4f}",
            ])
            j += 1

        headers_ult = [
            "Id vértice",
            "Posición (lat, lon)",
            "Fecha de creación",
            "Grullas (tags)",
            "Conteo de eventos",
            "Dist. prom. agua (km)",
        ]
        print(tabulate(rows_ult, headers=headers_ult, tablefmt="grid"))

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 1
    try:
        lat_o = float(input("Ingrese latitud de origen: "))
        lon_o = float(input("Ingrese longitud de origen: "))
        lat_d = float(input("Ingrese latitud de destino: "))
        lon_d = float(input("Ingrese longitud de destino: "))
        individuo = input("Ingrese el identificador del individuo: ")
    except:
        print("\nError: parámetros inválidos.")
        return

    res = lg.req_1(control, lat_o, lon_o, lat_d, lon_d, individuo)

    # Caso sin camino
    if res is None or "mensaje" in res:
        print("\n No se encontró un camino viable entre los puntos\n")
        print("Primer nodo donde aparece el individuo:", res["primer_nodo_del_individuo"])
        print("Tiempo de ejecución:", res.get("tiempo_ms", "Unknown"), " ms.")
        return

    print("Requerimiento 1"
          + "\nPrimer nodo donde aparece el individuo: " + str(res["primer_nodo_del_individuo"])
          + "\nDistancia total del camino: " + str(res["distancia_total"]) + " km"
          + "\nTotal de puntos en el camino: " + str(res["total_puntos"])
          + "\nTiempo de ejecución: " + str(res["tiempo_ms"]) + " ms.")

    # Función para armar la tabla
    def table(lista):
        headers = [
            "ID Punto",
            "Latitud",
            "Longitud",
            "# Individuos",
            "Primeros 3 individuos",
            "Últimos 3 individuos",
            "Distancia siguiente"
        ]

        filas = []
        for p in lista:
            fila = [
                p["punto_id"],
                p["latitud"],
                p["longitud"],
                p["num_individuos"],
                ", ".join(str(x) for x in p["primeros_3_individuos"]) if p["primeros_3_individuos"] else "—",
                ", ".join(str(x) for x in p["ultimos_3_individuos"]) if p["ultimos_3_individuos"] else "—",
                p["distancia_siguiente"]
            ]
            filas.append(fila)

        return tabulate(filas, headers, tablefmt="grid", stralign="center")

    print("\nPrimeros 5:\n")
    print(table(res["primeros_5"]))

    print("\nÚltimos 5:\n")
    print(table(res["ultimos_5"]))

def print_req_2(control):
    """
    Vista del Requerimiento 2.
    Pide los parámetros al usuario, llama a la lógica y muestra los resultados.
    """

    # Entrada de datos
    lat_origen = input("Ingrese la latitud del punto de origen: ")
    lon_origen = input("Ingrese la longitud del punto de origen: ")
    lat_destino = input("Ingrese la latitud del punto de destino: ")
    lon_destino = input("Ingrese la longitud del punto de destino: ")
    radio_km = input("Ingrese el radio del área de interés (km): ")

    # Medir tiempo de ejecución
    start_time = lg.get_time()
    result = lg.req_2(control, lat_origen, lon_origen, lat_destino, lon_destino, radio_km)
    end_time = lg.get_time()
    elapsed = lg.delta_time(start_time, end_time)

    print("\n===== Requerimiento 2 =====\n")

    # Verificar si hay camino o si hubo problema
    if not result.get("hay_camino", False):
        print("No fue posible encontrar un camino entre los puntos especificados.")
        mensaje = result.get("mensaje", None)
        if mensaje is not None:
            print("Detalle:", mensaje)
        print(f"\nTiempo de ejecución: {elapsed:.3f} ms\n")
        return

    # Resumen general
    print(f"Vértice de origen más cercano: {result['origen_id']}")
    print(f"Vértice de destino más cercano: {result['destino_id']}")
    print(f"Número de vértices en el camino: {result['num_vertices_camino']}")
    print(f"Distancia total recorrida (según grafo de movimiento): {result['distancia_total_km']:.3f} km")

    ultimo = result.get("ultimo_dentro_radio", None)
    if ultimo is None:
        print("Ningún vértice del camino quedó dentro del radio especificado.")
    else:
        print(
            "Último vértice del camino dentro del radio:",
            ultimo["id"],
            f"– distancia al origen: {ultimo['distancia_al_origen_km']:.3f} km"
        )

    print("\n--- Primeros 5 vértices del camino ---")
    primeros = result.get("primeros_5", [])
    if len(primeros) == 0:
        print("No hay vértices para mostrar.")
    else:
        rows_prim = []
        i = 0
        while i < len(primeros):
            v = primeros[i]
            tags_ini = ", ".join(v["tags_primeros"])
            tags_fin = ", ".join(v["tags_ultimos"])
            dist_sig = v["dist_siguiente_km"]
            if dist_sig is None:
                dist_sig_str = "N/A"
            else:
                dist_sig_str = f"{float(dist_sig):.3f}"
            rows_prim.append([
                v["id"],
                v["latitud"],
                v["longitud"],
                v["num_individuos"],
                tags_ini,
                tags_fin,
                dist_sig_str,
            ])
            i += 1

        headers = [
            "Id vértice",
            "Latitud",
            "Longitud",
            "# individuos",
            "Primeros tags",
            "Últimos tags",
            "Dist. al sig. (km)"
        ]
        print(tabulate(rows_prim, headers=headers, tablefmt="grid"))

    print("\n--- Últimos 5 vértices del camino ---")
    ultimos = result.get("ultimos_5", [])
    if len(ultimos) == 0:
        print("No hay vértices para mostrar.")
    else:
        rows_ult = []
        j = 0
        while j < len(ultimos):
            v = ultimos[j]
            tags_ini = ", ".join(v["tags_primeros"])
            tags_fin = ", ".join(v["tags_ultimos"])
            dist_sig = v["dist_siguiente_km"]
            if dist_sig is None:
                dist_sig_str = "N/A"
            else:
                dist_sig_str = f"{float(dist_sig):.3f}"
            rows_ult.append([
                v["id"],
                v["latitud"],
                v["longitud"],
                v["num_individuos"],
                tags_ini,
                tags_fin,
                dist_sig_str,
            ])
            j += 1

        headers = [
            "Id vértice",
            "Latitud",
            "Longitud",
            "# individuos",
            "Primeros tags",
            "Últimos tags",
            "Dist. al sig. (km)"
        ]
        print(tabulate(rows_ult, headers=headers, tablefmt="grid"))

    print(f"\nTiempo de ejecución: {elapsed:.3f} ms\n")


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    res = lg.req_3(control)
    
    if res == None:
        print("\n No se encontró una ruta migratoria viable dentro del nicho")
        return 
    
    print("Requerimiento 3"+
          "\nTotal de puntos en la ruta: "+str(res["total_puntos"])+
          "\nTotal de individuos en la ruta: "+str(res["total_individuos"])+
          "\nTiempo de ejecución: "+str(res["tiempo_ms"])+" ms.")
    
    def table(lista):
        headers = ["ID Punto",
            "Latitud",
            "Longitud",
            "# Individuos",
            "Primeros 3 individuos",
            "Últimos 3 individuos",
            "Distancia anterior",
            "Distancia siguiente"
        ]
        
        filas = []
        for p in lista:
            fila= [
                p["punto_id"],
                p["latitud"],
                p["longitud"],
                p["num_individuos"],
                ", ".join(str(x) for x in p["primeros_3_individuos"]) if p["primeros_3_individuos"] else "—",
                ", ".join(str(x) for x in p["ultimos_3_individuos"]) if p["ultimos_3_individuos"] else "—",
                p["distancia_anterior"],
                p["distancia_siguiente"]
            ]
            filas.append(fila)
        
        return tabulate(filas, headers, tablefmt="grid", stralign="center")
        
    print("Primeros 5:\n")
    print(table(res["primeros_5"]))
    
    print("\nUltimos 5:\n")
    print(table(res["ultimos_5"]))
    


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    try:
        lat_o = float(input("Ingrese latitud de origen: "))
        lon_o = float(input("Ingrese longitud de origen: "))
    except:
        print("\nError: parámetros inválidos.")
        return

    res = lg.req_4(control, lat_o, lon_o)

    # Caso sin red hídrica viable
    if res is None or "mensaje" in res:
        print("\n No existe una red hídrica viable desde el origen dado.\n")
        return

    print("Requerimiento 4"
          + "\nTotal de puntos en el corredor hídrico: " + str(res["total_puntos"])
          + "\nTotal de individuos en el corredor: " + str(res["total_individuos"])
          + "\nDistancia total a fuentes hídricas: " + str(res["distancia_total"]) + " km"
          + "\nTiempo de ejecución: " + str(res["tiempo_ms"]) + " ms.")

    # Función para armar la tabla
    def table(lista):
        headers = [
            "ID Punto",
            "Latitud",
            "Longitud",
            "# Individuos",
            "Primeros 3 individuos",
            "Últimos 3 individuos"
        ]

        filas = []
        for p in lista:
            fila = [
                p["punto_id"],
                p["latitud"],
                p["longitud"],
                p["num_individuos"],
                ", ".join(str(x) for x in p["primeros_3_individuos"]) if p["primeros_3_individuos"] else "—",
                ", ".join(str(x) for x in p["ultimos_3_individuos"]) if p["ultimos_3_individuos"] else "—"
            ]
            filas.append(fila)

        return tabulate(filas, headers, tablefmt="grid", stralign="center")

    print("\nPrimeros 5:\n")
    print(table(res["primeros_5"]))

    print("\nÚltimos 5:\n")
    print(table(res["ultimos_5"]))

def print_req_5(control):
    """
    Vista del Requerimiento 5.
    Pide los parámetros al usuario, llama a la lógica y muestra los resultados.
    """

    # Entrada de datos
    lat_origen = input("Ingrese la latitud del punto de origen: ")
    lon_origen = input("Ingrese la longitud del punto de origen: ")
    lat_destino = input("Ingrese la latitud del punto de destino: ")
    lon_destino = input("Ingrese la longitud del punto de destino: ")
    print('Criterio de optimización (por ejemplo "distancia" o "agua")')
    criterio = input("Ingrese el criterio: ")

    # Medir tiempo de ejecución
    start_time = lg.get_time()
    result = lg.req_5(control, lat_origen, lon_origen, lat_destino, lon_destino, criterio)
    end_time = lg.get_time()
    elapsed = lg.delta_time(start_time, end_time)

    print("\n===== Requerimiento 5 =====\n")

    if not result.get("hay_camino", False):
        print("No fue posible encontrar una ruta mínima entre los puntos especificados.")
        mensaje = result.get("mensaje", None)
        if mensaje is not None:
            print("Detalle:", mensaje)
        print(f"\nTiempo de ejecución: {elapsed:.3f} ms\n")
        return

    print(f"Grafo utilizado: {result['tipo_grafo']}")
    print(f"Vértice de origen más cercano: {result['origen_id']}")
    print(f"Vértice de destino más cercano: {result['destino_id']}")
    print(f"Número de vértices en el camino: {result['num_vertices_camino']}")
    print(f"Número de arcos en el camino: {result['num_arcos_camino']}")
    print(f"Costo total de la ruta (según criterio): {result['costo_total']:.3f}\n")

    # Primeros 5 vértices
    print("--- Primeros 5 vértices del camino ---")
    primeros = result.get("primeros_5", [])
    if len(primeros) == 0:
        print("No hay vértices para mostrar.")
    else:
        rows_prim = []
        i = 0
        while i < len(primeros):
            v = primeros[i]
            tags_ini = ", ".join(v["tags_primeros"])
            tags_fin = ", ".join(v["tags_ultimos"])
            dist_sig = v["dist_siguiente_km"]
            if dist_sig is None:
                dist_sig_str = "N/A"
            else:
                dist_sig_str = f"{float(dist_sig):.3f}"
            rows_prim.append([
                v["id"],
                v["latitud"],
                v["longitud"],
                v["num_individuos"],
                tags_ini,
                tags_fin,
                dist_sig_str,
            ])
            i += 1

        headers = [
            "Id vértice",
            "Latitud",
            "Longitud",
            "# individuos",
            "Primeros tags",
            "Últimos tags",
            "Dist. al sig. (km)"
        ]
        print(tabulate(rows_prim, headers=headers, tablefmt="grid"))

    # Últimos 5 vértices
    print("\n--- Últimos 5 vértices del camino ---")
    ultimos = result.get("ultimos_5", [])
    if len(ultimos) == 0:
        print("No hay vértices para mostrar.")
    else:
        rows_ult = []
        j = 0
        while j < len(ultimos):
            v = ultimos[j]
            tags_ini = ", ".join(v["tags_primeros"])
            tags_fin = ", ".join(v["tags_ultimos"])
            dist_sig = v["dist_siguiente_km"]
            if dist_sig is None:
                dist_sig_str = "N/A"
            else:
                dist_sig_str = f"{float(dist_sig):.3f}"
            rows_ult.append([
                v["id"],
                v["latitud"],
                v["longitud"],
                v["num_individuos"],
                tags_ini,
                tags_fin,
                dist_sig_str,
            ])
            j += 1

        headers = [
            "Id vértice",
            "Latitud",
            "Longitud",
            "# individuos",
            "Primeros tags",
            "Últimos tags",
            "Dist. al sig. (km)"
        ]
        print(tabulate(rows_ult, headers=headers, tablefmt="grid"))

    print(f"\nTiempo de ejecución: {elapsed:.3f} ms\n")


def print_req_6(control):
    """
    Imprime la solución del Requerimiento 6 en formato textual estilo reporte.
    """
    res = lg.req_6(control)

    print("\n==========  REQUERIMIENTO 6: SUBREDES HÍDRICAS  ==========\n")

    if res is None or res["total_subredes"] == 0:
        print("No fue posible identificar subredes hídricas en el grafo.")
        return

    print(f"Total de subredes hídricas identificadas: {res['total_subredes']}\n")
    print(f"Mostrando las 5 subredes más grandes (o menos si no existen 5):\n")

    for sub in res["subredes_top"]:
        print(f"----- Subred {sub['id_subred']} -----")
        print(f"- Latitud mínima: {sub['lat_min']}")
        print(f"- Latitud máxima: {sub['lat_max']}")
        print(f"- Longitud mínima: {sub['lon_min']}")
        print(f"- Longitud máxima: {sub['lon_max']}")
        print(f"- Total puntos migratorios: {sub['num_puntos']}")
        print(f"- Total individuos identificados: {sub['num_individuos']}\n")

        # ---- Primeros puntos ----
        print("Primeros 3 puntos migratorios:")
        if len(sub["primeros_puntos"]) == 0:
            print("  * No disponibles")
        else:
            for p in sub["primeros_puntos"]:
                print(f"  * ID: {p['id']}, lat: {p['latitud']}, lon: {p['longitud']}")
        print()

        # ---- Últimos puntos ----
        print("Últimos 3 puntos migratorios:")
        if len(sub["ultimos_puntos"]) == 0:
            print("  * No disponibles")
        else:
            for p in sub["ultimos_puntos"]:
                print(f"  * ID: {p['id']}, lat: {p['latitud']}, lon: {p['longitud']}")
        print()

        # ---- Primeros individuos ----
        print("Primeros 3 individuos (tags):")
        if len(sub["primeros_individuos"]) == 0:
            print("  * No disponibles")
        else:
            for ind in sub["primeros_individuos"]:
                print(f"  * {ind}")
        print()

        # ---- Últimos individuos ----
        print("Últimos 3 individuos (tags):")
        if len(sub["ultimos_individuos"]) == 0:
            print("  * No disponibles")
        else:
            for ind in sub["ultimos_individuos"]:
                print(f"  * {ind}")
        print("------------------------------------------------------------\n")

    print(f"Tiempo de ejecución: {res['tiempo_ms']:.3f} ms.\n")



# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 6:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
