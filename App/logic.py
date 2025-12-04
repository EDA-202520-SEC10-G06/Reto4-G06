import time
import math
from tabulate import tabulate
from DataStructures.Priority_queue import pq_entry as pqe
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import array_list as al
from datetime import datetime
from DataStructures.List import single_linked_list as lt
from DataStructures.Map import map_linear_probing as m
from DataStructures.Graph import digraph as G
from DataStructures.Queue import queue as q
from DataStructures.Graph import dijkstra as dj
from DataStructures.Stack import stack as stack
from DataStructures.Graph import bfs as bfs
from DataStructures.Graph import edge as E
import csv

import os

data_dir = os.path.dirname(os.path.realpath('__file__')) + '/Data/'

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos

    data={
        "movement":None,
        "sorted_info":None,
        "water":None,
        "map":None
    }
    data['sorted_info']=al.new_list()
    data['movement']=G.new_graph(6000)
    data['water']=G.new_graph(6000)
    data['map']=m.new_map(10000,0.8)
    return data


# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """

    # TODO: Realizar la carga de datos

    load_info(catalog,filename)
    al.quick_sort(catalog["sorted_info"],sort_criteria)
    first_graph(catalog)
    edges_graph_1(catalog)
    edges_graph_2(catalog)
    return catalog

# Anexas a la carga de datos

def sort_criteria(element_1,element_2):
    if element_1["timestamp"]<element_2["timestamp"]:
        return True
    return False

def load_info(catalog,filename):
    file=data_dir + filename
    input_file=csv.DictReader(open(file,encoding="utf-8"))
    for crane in input_file:
        crane["timestamp"]=datetime.strptime(crane["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        add_info(catalog,crane)
    return al.size(catalog["sorted_info"])

def add_info(catalog,crane):
    al.add_last(catalog["sorted_info"],crane)
    
def first_graph(catalog):
    first_element=al.first_element(catalog["sorted_info"])
    insert_vertex_format(catalog,first_element)
    m.put(catalog["map"],first_element["event-id"],first_element["event-id"])
    for i in range(1,al.size(catalog["sorted_info"])):
        crane=al.get_element(catalog["sorted_info"],i)
        vertex_keys=G.vertices(catalog["movement"])
        for j in range(al.size(vertex_keys)):
            vertex_key=al.get_element(vertex_keys,j)
            vertex=G.get_vertex(catalog["movement"],vertex_key)
            distance=haversine(float(crane["location-lat"]),float(crane["location-long"]),float(vertex["value"]["Posicion"][0]),float(vertex["value"]["Posicion"][1]))
            time_diff=abs(crane["timestamp"]-vertex["value"]["Tiempo_creacion"]).total_seconds()/3600
            if distance<3 and time_diff<3:
                vertex["value"]["cantidad"]+=1
                vertex["value"]["distancia_prom"]+=int(crane["comments"])
                al.add_last(vertex["value"]["Eventos"],(crane["event-id"],crane))
                if crane["tag-local-identifier"] not in vertex["value"]["I_individuo"]["elements"]:
                    al.add_last(vertex["value"]["I_individuo"],crane["tag-local-identifier"])
                m.put(catalog["map"],crane["event-id"],vertex_key)
                break
        else:
            insert_vertex_format(catalog,crane)
            m.put(catalog["map"],crane["event-id"],crane["event-id"])
    return catalog
            
def insert_vertex_format(catalog, element):
    vertex_id = element["event-id"]
    individuo=al.new_list()
    events=al.new_list()
    al.add_last(individuo,element["tag-local-identifier"])
    al.add_last(events,(element["event-id"], element))
    data = {
        "Posicion": (element["location-lat"], element["location-long"]),
        "Tiempo_creacion": element["timestamp"],
        "I_individuo": individuo,
        "Eventos": events,
        "cantidad": 1,
        "distancia_prom": int(element["comments"])
    }
    G.insert_vertex(catalog["movement"], vertex_id, data)
    G.insert_vertex(catalog["water"], vertex_id, data)
    return vertex_id

def edges_graph_1(catalog):
    dicc={}
    for i in range(al.size(catalog["sorted_info"])):
        crane=al.get_element(catalog["sorted_info"],i)
        if crane["tag-local-identifier"] not in dicc:
            dicc[crane["tag-local-identifier"]]=al.new_list()
            al.add_last(dicc[crane["tag-local-identifier"]],crane)
        else:
            al.add_last(dicc[crane["tag-local-identifier"]],crane)
    trips={}
    for tag in dicc:
        lista=dicc[tag]
        if al.size(lista)<=1:
            continue
        prev_event=al.get_element(lista,0)
        prev_id=prev_event["event-id"]
        prev_id_vertex=m.get(catalog["map"],prev_id)
        for index in range(1,al.size(lista)):

            current=al.get_element(lista,index)
            current_id=current["event-id"]
            current_id_vertex=m.get(catalog["map"],current_id)

            if prev_id_vertex is None or current_id_vertex is None:
                prev_event=current
                prev_id_vertex=current_id_vertex
                continue
            if prev_id_vertex != current_id_vertex:
                vertex_a=G.get_vertex(catalog["movement"],prev_id_vertex)
                vertex_b=G.get_vertex(catalog["movement"],current_id_vertex)
                lat_a,lon_a=vertex_a["value"]["Posicion"]
                lat_b,lon_b=vertex_b["value"]["Posicion"]
                distance=haversine(float(lat_a), float(lon_a), float(lat_b), float(lon_b))
                key=(prev_id_vertex,current_id_vertex)
                if key not in trips:
                    trips[key]={"suma":0.0,"count":0}
                trips[key]["suma"]+=distance
                trips[key]["count"]+=1
                prev_event=current
                prev_id_vertex=current_id_vertex
    for (A,B),info in trips.items():
        peso=info["suma"]/info["count"]
        G.add_edge(catalog["movement"],A,B,peso)
    return catalog

def edges_graph_2(catalog):
    dicc = {}
    for i in range(al.size(catalog["sorted_info"])):
        crane = al.get_element(catalog["sorted_info"], i)
        tag = crane["tag-local-identifier"]
        if tag not in dicc:
            dicc[tag] = al.new_list()
        al.add_last(dicc[tag], crane)
    links = {}
    for tag in dicc:
        lista = dicc[tag]

        if al.size(lista) <= 1:
            continue
        prev_event = al.get_element(lista, 0)
        prev_id = prev_event["event-id"]
        prev_entry = m.get(catalog["map"], prev_id)
        if prev_entry is None:
            prev_id_vertex = None
        else:
            prev_id_vertex = prev_entry
        for index in range(1, al.size(lista)):
            current = al.get_element(lista, index)
            current_id = current["event-id"]
            curr_entry = m.get(catalog["map"], current_id)
            if curr_entry is None:
                current_id_vertex = None
            else:
                current_id_vertex = curr_entry
            if prev_id_vertex is None or current_id_vertex is None:
                prev_id_vertex = current_id_vertex
                continue
            if prev_id_vertex != current_id_vertex:
                A = prev_id_vertex
                B = current_id_vertex
                vertex_B = G.get_vertex(catalog["movement"], B)
                suma_agua = vertex_B["value"]["distancia_prom"] 
                cant = vertex_B["value"]["cantidad"]
                if cant > 0:
                    prom_agua_km = (suma_agua / cant) / 1000.0
                else:
                    prom_agua_km = 0
                key = (A, B)
                if key not in links:
                    links[key] = prom_agua_km
            prev_id_vertex = current_id_vertex
    for (A, B), peso in links.items():
        G.add_edge(catalog["water"], A, B, peso)
    return catalog

def get_load_info(catalog):
    """
    Retorna un resumen de la carga de datos para los DOS grafos.

    Incluye:
      - total_grullas: número de grullas distintas (tag-local-identifier)
      - total_eventos: número total de registros cargados
      - total_nodos: número de vértices en el grafo de movimientos
      - total_arcos_mov: número de arcos en el grafo de movimientos
      - total_arcos_agua: número de arcos en el grafo de proximidad al agua
      - primeros_5: primeros 5 nodos creados (por fecha de creación)
      - ultimos_5: últimos 5 nodos creados (por fecha de creación)

    Cada nodo en primeros_5/ultimos_5 es un diccionario con:
      - id
      - lat
      - lon
      - fecha
      - tags (lista de tags)
      - num_eventos
      - dist_agua_prom_km
    """

    # Total de eventos
    total_eventos = al.size(catalog["sorted_info"])

    # Total de grullas distintas (tag-local-identifier)
    grullas = set()
    i = 0
    while i < total_eventos:
        crane = al.get_element(catalog["sorted_info"], i)
        tag = crane.get("tag-local-identifier")
        if tag is not None:
            grullas.add(tag)
        i += 1
    total_grullas = len(grullas)

    # Totales de nodos y arcos en cada grafo
    total_nodos = G.num_vertices(catalog["movement"])
    total_arcos_mov = G.num_edges(catalog["movement"])
    total_arcos_agua = G.num_edges(catalog["water"])

    # Construir lista con la info de TODOS los vértices, ordenados por fecha
    vertices = G.vertices(catalog["movement"])
    n_vertices = al.size(vertices)

    vertices_info = []
    i = 0
    while i < n_vertices:
        vid = al.get_element(vertices, i)
        vertex = G.get_vertex(catalog["movement"], vid)
        if vertex is not None:
            data = vertex["value"]
            lat, lon = data["Posicion"]
            fecha = data["Tiempo_creacion"]

            individuos = data["I_individuo"]
            tags = []
            j = 0
            while j < al.size(individuos):
                tags.append(al.get_element(individuos, j))
                j += 1

            eventos = data["Eventos"]
            num_eventos_nodo = al.size(eventos)

            cant = data["cantidad"]
            if cant > 0:
                dist_agua_prom_km = (float(data["distancia_prom"]) / float(cant)) / 1000.0
            else:
                dist_agua_prom_km = 0.0

            vertices_info.append({
                "id": vid,
                "lat": lat,
                "lon": lon,
                "fecha": fecha,
                "tags": tags,
                "num_eventos": num_eventos_nodo,
                "dist_agua_prom_km": dist_agua_prom_km
            })
        i += 1

    # Ordenar por fecha de creación (del nodo más antiguo al más reciente)
    vertices_info.sort(key=lambda v: v["fecha"])

    if len(vertices_info) > 5:
        primeros_5 = vertices_info[:5]
        ultimos_5 = vertices_info[-5:]
    else:
        primeros_5 = vertices_info[:]
        ultimos_5 = vertices_info[:]

    resumen = {
        "total_grullas": total_grullas,
        "total_eventos": total_eventos,
        "total_nodos": total_nodos,
        "total_arcos_mov": total_arcos_mov,
        "total_arcos_agua": total_arcos_agua,
        "primeros_5": primeros_5,
        "ultimos_5": ultimos_5,
    }

    return resumen

def haversine(lat_a, lon_a, lat_b, lon_b):
    R = 6371  # radio de la Tierra en km
    lat_a, lon_a, lat_b, lon_b = map(math.radians, [lat_a, lon_a, lat_b, lon_b])
    dlat = lat_b - lat_a
    dlon = lon_b - lon_a

    # fórmula de haversine correcta
    a = (math.sin(dlat / 2) ** 2
         + math.cos(lat_a) * math.cos(lat_b) * math.sin(dlon / 2) ** 2)

    # por si algún error numérico hace que a se pase un poquito de 1
    if a > 1:
        a = 1.0
    elif a < 0:
        a = 0.0

    c = 2 * math.asin(math.sqrt(a))
    return R * c

def nearest_vertex_by_coord(catalog, lat, lon):
    """
    Retorna el id del vértice más cercano a las coordenadas dadas
    y la distancia Haversine en km.
    Usa el grafo de movimientos (catalog["movement"]), que tiene
    los mismos vértices que el grafo de agua.
    """
    lat = float(lat)
    lon = float(lon)

    graph = catalog["movement"]
    vertices = G.vertices(graph)
    n = al.size(vertices)

    mejor_id = None
    mejor_dist = float("inf")

    i = 0
    while i < n:
        vid = al.get_element(vertices, i)
        vertex = G.get_vertex(graph, vid)
        if vertex is not None:
            data = vertex["value"]
            try:
                vlat = float(data["Posicion"][0])
                vlon = float(data["Posicion"][1])
                dist = haversine(lat, lon, vlat, vlon)
                if dist < mejor_dist:
                    mejor_dist = dist
                    mejor_id = vid
            except Exception:
                # si falta info de posición, se ignora ese vértice
                pass
        i += 1

    return mejor_id, mejor_dist


def _get_edge_weight(graph, u, v):
    """
    Retorna el peso del arco u->v en el grafo dado.
    Si no existe el arco, retorna None.
    """
    vertex_u = G.get_vertex(graph, u)
    if vertex_u is None:
        return None

    adj = vertex_u["value"]["adjacents"]
    info = m.get(adj, v)
    if info is None:
        return None

    return info["weight"]


def _resumen_camino_req5(catalog, graph_key, camino, visited_map):
    """
    Construye la lista de diccionarios con la info de cada vértice en el camino
    para Req 2 y Req 5.

    Cada elemento del resultado tiene:
      - id
      - latitud
      - longitud
      - num_individuos
      - tags_primeros (lista)
      - tags_ultimos (lista)
      - dist_siguiente_km (float, 0.0 para el último vértice)
    """
    graph = catalog[graph_key]
    n = len(camino)
    resumen = []

    idx = 0
    while idx < n:
        vid = camino[idx]
        vertex = G.get_vertex(graph, vid)

        # Valores por defecto
        lat = "Unknown"
        lon = "Unknown"
        num_individuos = 0
        tags_primeros = []
        tags_ultimos = []

        if vertex is not None:
            data = vertex["value"]

            # Lat / Lon
            try:
                lat = float(data["Posicion"][0])
                lon = float(data["Posicion"][1])
            except Exception:
                lat = "Unknown"
                lon = "Unknown"

            # Lista de individuos (I_individuo es un array_list de tags)
            individuos = data.get("I_individuo", None)
            if individuos is not None:
                num_individuos = al.size(individuos)

                # primeros 3 tags
                j = 0
                while j < num_individuos and j < 3:
                    tags_primeros.append(al.get_element(individuos, j))
                    j += 1

                # últimos 3 tags
                if num_individuos <= 3:
                    k = 0
                    while k < num_individuos:
                        tags_ultimos.append(al.get_element(individuos, k))
                        k += 1
                else:
                    start = num_individuos - 3
                    k = start
                    while k < num_individuos:
                        tags_ultimos.append(al.get_element(individuos, k))
                        k += 1

        # Distancia al siguiente vértice en la ruta
        if idx == n - 1:
            dist_sig = 0.0
        else:
            vid_sig = camino[idx + 1]
            edge = G.get_edge(graph, vid, vid_sig)
            if edge is not None:
                try:
                    dist_sig = float(E.weight(edge))
                except Exception:
                    # fallback: diferencia de distancias en Dijkstra
                    info_v = m.get(visited_map, vid)
                    info_sig = m.get(visited_map, vid_sig)
                    dist_sig = float(info_sig["dist_to"] - info_v["dist_to"])
            else:
                # si no encontramos el arco: diferencia de distancias en Dijkstra (si existe)
                info_v = m.get(visited_map, vid)
                info_sig = m.get(visited_map, vid_sig)
                dist_sig = float(info_sig["dist_to"] - info_v["dist_to"])

        info_v = {
            "id": vid,
            "latitud": lat,
            "longitud": lon,
            "num_individuos": num_individuos,
            "tags_primeros": tags_primeros,
            "tags_ultimos": tags_ultimos,
            "dist_siguiente_km": dist_sig
        }

        resumen.append(info_v)
        idx += 1

    return resumen

# Funciones de consulta sobre el catálogo


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog, lat_origen, lon_origen, lat_destino, lon_destino, radio_km):
    """
    REQ 2: Detectar los movimientos de un nicho biológico alrededor de un área.

    Parámetros:
        - lat_origen, lon_origen: coordenadas del punto de origen.
        - lat_destino, lon_destino: coordenadas del punto destino.
        - radio_km: radio del área de interés (km).

    Retorna un diccionario con:
        - "origen_id", "destino_id"
        - "ultimo_dentro_radio": { "id", "distancia_al_origen_km" } o None
        - "distancia_total_km"
        - "num_vertices_camino"
        - "camino": lista de ids de vértice
        - "resumen_vertices": lista de diccionarios (uno por vértice)
          y además "primeros_5" y "ultimos_5".
    """
    
    pass



def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog, lat_origen, lon_origen, lat_destino, lon_destino, criterio):
    """
    REQ 5: Encontrar la ruta óptima entre dos ubicaciones
    según el criterio de proximidad al agua o distancia mínima.

    Parámetros:
        - lat_origen, lon_origen: coordenadas del punto de origen (strings o floats).
        - lat_destino, lon_destino: coordenadas del punto destino.
        - criterio: "agua" o "distancia".

    Retorna un diccionario con:
        - "hay_camino" (bool)
        - "tipo_grafo" ("movement" o "water")
        - "origen_id", "destino_id"
        - "costo_total" (float)
        - "num_vertices_camino", "num_arcos_camino"
        - "camino": lista de ids de vértice
        - "primeros_5", "ultimos_5"
        - "resumen_vertices"
        - "mensaje"
    """

    # 1. Buscar los vértices migratorios más cercanos a las coordenadas usando Haversine
    origen_id, dist_origen = nearest_vertex_by_coord(catalog, lat_origen, lon_origen)
    destino_id, dist_destino = nearest_vertex_by_coord(catalog, lat_destino, lon_destino)

    # Estructura base del resultado
    resultado = {
        "hay_camino": False,
        "tipo_grafo": None,
        "origen_coord": (lat_origen, lon_origen),
        "destino_coord": (lat_destino, lon_destino),
        "criterio": criterio,
        "origen_id": origen_id,
        "destino_id": destino_id,
        "costo_total": None,
        "num_vertices_camino": 0,
        "num_arcos_camino": 0,
        "camino": [],
        "resumen_vertices": [],
        "primeros_5": [],
        "ultimos_5": [],
        "mensaje": ""
    }

    # Si no se encontró vértice cercano para origen o destino, no se puede seguir
    if origen_id is None or destino_id is None:
        resultado["mensaje"] = (
            "No se encontró un punto migratorio cercano para el origen o el destino, "
            "por lo que no es posible calcular una ruta."
        )
        return resultado

    # 2. Seleccionar el grafo según el criterio del usuario
    if criterio == "agua":
        graph_key = "water"
        resultado["tipo_grafo"] = "water"
    else:
        # Por defecto (o si se pasa "distancia" u otra cosa) usamos el grafo de desplazamiento
        graph_key = "movement"
        resultado["tipo_grafo"] = "movement"

    graph = catalog[graph_key]

    # 3. Ejecutar Dijkstra desde el vértice de origen
    aux_structure = dj.dijkstra(graph, origen_id)

    # 4. Verificar si existe camino hacia el destino
    if not dj.has_path_to(destino_id, aux_structure):
        resultado["mensaje"] = (
            "No se reconoce un camino viable entre los puntos migratorios "
            "de origen y destino bajo el criterio seleccionado."
        )
        return resultado

    # 5. Reconstruir el camino usando path_to (retorna un stack origen->destino al hacer pop)
    path_stack = dj.path_to(destino_id, aux_structure)
    if path_stack is None:
        resultado["mensaje"] = (
            "No se reconoce un camino viable entre los puntos migratorios "
            "de origen y destino bajo el criterio seleccionado."
        )
        return resultado

    camino = []
    while not stack.is_empty(path_stack):
        v = stack.pop(path_stack)
        camino.append(v)

    num_vertices = len(camino)
    num_arcos = num_vertices - 1 if num_vertices > 0 else 0

    if num_vertices == 0:
        resultado["mensaje"] = (
            "No se reconoce un camino viable entre los puntos migratorios "
            "de origen y destino bajo el criterio seleccionado."
        )
        return resultado

    # 6. Costo total (distancia o "costo" según el grafo usado)
    distancia_total = dj.dist_to(destino_id, aux_structure)
    if distancia_total is None:
        resultado["mensaje"] = (
            "No se pudo obtener el costo total de la ruta entre los puntos migratorios "
            "de origen y destino bajo el criterio seleccionado."
        )
        return resultado

    # 7. Resumen detallado de cada vértice de la ruta
    visited_map = aux_structure["visited"]
    resumen_vertices = _resumen_camino_req5(catalog, graph_key, camino, visited_map)

    # 8. Primeros cinco y últimos cinco vértices
    if num_vertices > 5:
        primeros_5 = resumen_vertices[:5]
        ultimos_5 = resumen_vertices[-5:]
    else:
        primeros_5 = resumen_vertices[:]
        ultimos_5 = resumen_vertices[:]

    # 9. Completar el diccionario de respuesta
    resultado["hay_camino"] = True
    resultado["costo_total"] = float(distancia_total)
    resultado["num_vertices_camino"] = num_vertices
    resultado["num_arcos_camino"] = num_arcos
    resultado["camino"] = camino
    resultado["resumen_vertices"] = resumen_vertices
    resultado["primeros_5"] = primeros_5
    resultado["ultimos_5"] = ultimos_5
    resultado["mensaje"] = "Ruta óptima calculada correctamente."

    return resultado


def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
