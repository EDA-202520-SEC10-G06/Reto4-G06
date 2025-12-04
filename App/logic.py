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
from DataStructures.Graph import dfo as DFO
from DataStructures.Queue import queue as q
from DataStructures.Graph import dijkstra as dj
from DataStructures.Stack import stack as stack
from DataStructures.Graph import bfs as bfs
from DataStructures.Graph import edge as E
from DataStructures.Graph import dfo_structure
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

def peso_haversine(graph, u, v):
    Vu = G.get_vertex(graph, u)
    Vv = G.get_vertex(graph, v)
    
    if Vu == None or Vv == None:
        return 0
    
    posU = Vu["value"]["Posicion"]
    posV = Vv["value"]["Posicion"]
    
    if posU == None or posV == None:
        return 0
    
    latu, lonu = posU
    latv, lonv = posV

    if None in (latu, lonu, latv, lonv):
        return 0

    return haversine(latu, lonu, latv, lonv)

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



def dfs_path(graph, source, target):
    stack_ds = stack.new_stack()
    visited = set()
    parent = {}

    stack.push(stack_ds, source)
    visited.add(source)
    parent[source] = None

    while not stack.is_empty(stack_ds):
        v = stack.pop(stack_ds)
        if v == target:
            break

        adj = G.adjacents(graph, v)
        for i in range(al.size(adj)):
            nxt = al.get_element(adj, i)
            if nxt not in visited:
                visited.add(nxt)
                parent[nxt] = v
                stack.push(stack_ds, nxt)

    # reconstrucción del camino
    if target not in parent:
        return None

    path = al.new_list()
    cur = target
    while cur is not None:
        al.add_first(path, cur)
        cur = parent[cur]

    return path

def format_vertex(graph, vid, next_vid):
    v = G.get_vertex(graph, vid)
    val = v["value"]

    lat, lon = val["Posicion"]
    individuo_list = val["I_individuo"]
    total = al.size(individuo_list)

    primeros = []
    ultimos = []

    for i in range(min(3, total)):
        primeros.append(al.get_element(individuo_list, i))

    for i in range(max(0, total-3), total):
        ultimos.append(al.get_element(individuo_list, i))

    # distancia al siguiente
    dist_next = "Unknown"
    if next_vid is not None:
        e = G.get_edge(graph, vid, next_vid)
        if e:
            dist_next = e["weight"]

    return {
        "punto_id": vid,
        "latitud": lat,
        "longitud": lon,
        "num_individuos": total,
        "primeros_3_individuos": primeros,
        "ultimos_3_individuos": ultimos,
        "distancia_siguiente": dist_next
    }

def prim_mst(graph, start):
    # Obtener todos los vértices
    vertices = G.vertices(graph)
    num_vertices = al.size(vertices)

    visited = set()
    parent = {}
    dist = {}

    # Cola de prioridad (min-heap por defecto)
    pq_ds = pq.new_heap()

    # Inicializar distancias y padres
    for i in range(al.size(vertices)):
        vid = al.get_element(vertices, i)
        dist[vid] = float("inf")
        parent[vid] = None

    # El vértice de inicio tiene distancia 0
    dist[start] = 0
    pq.insert(pq_ds, 0, start)   # priority = 0, value = start

    while not pq.is_empty(pq_ds):
        # remove() en tu priority_queue devuelve SOLO el value (el vértice)
        u = pq.remove(pq_ds)

        if u in visited:
            continue
        visited.add(u)

        # recorrer adyacentes
        adj = G.adjacents(graph, u)
        if adj is None:
            continue

        for i in range(al.size(adj)):
            v = al.get_element(adj, i)

            # tratar aristas como NO DIRIGIDAS
            edge = G.get_edge(graph, u, v)
            if edge:
                w = edge["weight"]
                if v not in visited and w < dist[v]:
                    dist[v] = w
                    parent[v] = u
                    # en tu priority_queue: insert(heap, priority, value)
                    pq.insert(pq_ds, w, v)

            # revisar arista inversa (v → u)
            rev = G.get_edge(graph, v, u)
            if rev:
                w = rev["weight"]
                if v not in visited and w < dist[v]:
                    dist[v] = w
                    parent[v] = u
                    pq.insert(pq_ds, w, v)

    # si solo se visitó un nodo → no hay MST real
    if len(visited) <= 1:
        return None

    return parent, dist, visited


# Funciones de consulta sobre el catálogo


def req_1(catalog, lat_o, lon_o, lat_d, lon_d, individuo):
    """
    Retorna el resultado del requerimiento 1
    """

    start = get_time()

    graph = catalog["movement"]

    # 1. encontrar nodo origen y destino (OJO: nearest_vertex_by_coord devuelve (id, dist))
    origen_id, _ = nearest_vertex_by_coord(catalog, lat_o, lon_o)
    destino_id, _ = nearest_vertex_by_coord(catalog, lat_d, lon_d)

    # 2. encontrar primer nodo donde aparece el individuo
    vertices = G.vertices(graph)
    first_node = "Unknown"
    for i in range(al.size(vertices)):
        vid = al.get_element(vertices, i)
        v = G.get_vertex(graph, vid)
        lista = v["value"]["I_individuo"]
        for j in range(al.size(lista)):
            if al.get_element(lista, j) == individuo:
                first_node = vid
                break
        if first_node != "Unknown":
            break

    # 3. buscar camino con DFS
    path = dfs_path(graph, origen_id, destino_id)
    if path is None:
        end = get_time()
        return {
            "primer_nodo_del_individuo": first_node,
            "mensaje": "No existe un camino viable entre los puntos",
            "tiempo_ms": delta_time(start, end)
        }

    total_pts = al.size(path)

    # 4. calcular distancia total
    dist_total = 0.0
    for i in range(total_pts - 1):
        A = al.get_element(path, i)
        B = al.get_element(path, i + 1)
        e = G.get_edge(graph, A, B)
        if e:
            dist_total += e["weight"]  # o float(E.weight(e)) 

    # 5. primeros 5 y últimos 5
    primeros = []
    ultimos = []

    for i in range(min(5, total_pts)):
        vid = al.get_element(path, i)
        next_vid = al.get_element(path, i + 1) if i + 1 < total_pts else None
        primeros.append(format_vertex(graph, vid, next_vid))

    for i in range(max(0, total_pts - 5), total_pts):
        vid = al.get_element(path, i)
        next_vid = al.get_element(path, i + 1) if i + 1 < total_pts else None
        ultimos.append(format_vertex(graph, vid, next_vid))

    end = get_time()

    return {
        "primer_nodo_del_individuo": first_node,
        "distancia_total": dist_total,
        "total_puntos": total_pts,
        "primeros_5": primeros,
        "ultimos_5": ultimos,
        "tiempo_ms": delta_time(start, end)
    }


def req_2(catalog, lat_origen, lon_origen, lat_destino, lon_destino, radio_km):
    """
    REQ 2: Detectar los movimientos de un nicho biológico alrededor de un área.

    Parámetros:
        - lat_origen, lon_origen: coordenadas del punto de origen (strings o floats).
        - lat_destino, lon_destino: coordenadas del punto de destino.
        - radio_km: radio del área de interés en km (desde el punto de origen).

    Retorna un diccionario con:
        - "hay_camino" (bool)
        - "origen_id", "destino_id"
        - "distancia_total_km" (suma de pesos en el grafo de movimiento)
        - "num_vertices_camino"
        - "camino": lista de ids de vértice
        - "ultimo_dentro_radio": {"id", "distancia_al_origen_km"} o None
        - "resumen_vertices": info completa de cada vértice
        - "primeros_5", "ultimos_5"
        - "mensaje"
    """

    # Convertir radio a float
    radio_km = float(radio_km)

    # 1. Encontrar vértices de origen y destino más cercanos (Haversine + nearest_vertex_by_coord)
    origen_id, dist_origen = nearest_vertex_by_coord(catalog, lat_origen, lon_origen)
    destino_id, dist_destino = nearest_vertex_by_coord(catalog, lat_destino, lon_destino)

    resultado = {
        "hay_camino": False,
        "origen_id": origen_id,
        "destino_id": destino_id,
        "distancia_total_km": None,
        "num_vertices_camino": 0,
        "camino": [],
        "resumen_vertices": [],
        "primeros_5": [],
        "ultimos_5": [],
        "ultimo_dentro_radio": None,
        "mensaje": ""
    }

    # Si no se pudo asociar alguno de los puntos a un vértice, no hay ruta
    if origen_id is None or destino_id is None:
        resultado["mensaje"] = (
            "No se encontró un punto migratorio cercano para el origen o el destino, "
            "por lo que no es posible calcular un camino."
        )
        return resultado

    graph = catalog["movement"]

    # 2. Ejecutar BFS desde el vértice de origen (restricción del enunciado)
    bfs_search = bfs.bfs(graph, origen_id)

    # 3. Verificar si hay camino hasta el destino
    if not bfs.has_path_to(destino_id, bfs_search):
        resultado["mensaje"] = (
            "No se reconoce un camino viable entre los puntos migratorios "
            "de origen y destino utilizando BFS en el grafo de movimiento."
        )
        return resultado

    # 4. Reconstruir el camino origen -> destino usando path_to (devuelve un stack)
    path_stack = bfs.path_to(destino_id, bfs_search)
    if path_stack is None:
        resultado["mensaje"] = (
            "No se reconoce un camino viable entre los puntos migratorios "
            "de origen y destino utilizando BFS en el grafo de movimiento."
        )
        return resultado

    camino = []
    while not stack.is_empty(path_stack):
        v = stack.pop(path_stack)
        camino.append(v)

    num_vertices = len(camino)
    if num_vertices == 0:
        resultado["mensaje"] = (
            "No se reconoce un camino viable entre los puntos migratorios "
            "de origen y destino utilizando BFS en el grafo de movimiento."
        )
        return resultado

    # 5. Calcular distancia total del camino (suma de pesos de los arcos en el grafo de movimiento)
    distancia_total = 0.0
    i = 0
    while i < num_vertices - 1:
        u = camino[i]
        v = camino[i + 1]
        edge = G.get_edge(graph, u, v)
        if edge is not None:
            try:
                distancia_total += float(E.weight(edge))
            except Exception:
                # si algo falla con el peso, simplemente no sumamos ese tramo
                pass
        i += 1

    # 6. Determinar el último vértice dentro del radio desde el origen (usando Haversine)
    try:
        lat0 = float(lat_origen)
        lon0 = float(lon_origen)
    except Exception:
        # Si por alguna razón no se pueden convertir, usamos la posición del vértice de origen
        vert_origen = G.get_vertex(graph, origen_id)
        if vert_origen is not None:
            try:
                lat0 = float(vert_origen["value"]["Posicion"][0])
                lon0 = float(vert_origen["value"]["Posicion"][1])
            except Exception:
                lat0, lon0 = 0.0, 0.0
        else:
            lat0, lon0 = 0.0, 0.0

    ultimo_dentro = None

    for vid in camino:
        vert = G.get_vertex(graph, vid)
        if vert is None:
            continue
        data = vert["value"]
        try:
            vlat = float(data["Posicion"][0])
            vlon = float(data["Posicion"][1])
        except Exception:
            # si no hay posición válida, no se considera para el radio
            continue

        dist_al_origen = haversine(lat0, lon0, vlat, vlon)
        if dist_al_origen <= radio_km:
            ultimo_dentro = {
                "id": vid,
                "distancia_al_origen_km": dist_al_origen
            }

    # 7. Construir resumen de vértices (igual formato que Req 5)
    resumen_vertices = []
    i = 0
    while i < num_vertices:
        vid = camino[i]
        vert = G.get_vertex(graph, vid)

        # Valores por defecto
        lat = "Unknown"
        lon = "Unknown"
        num_individuos = 0
        tags_primeros = []
        tags_ultimos = []
        dist_sig = 0.0

        if vert is not None:
            data = vert["value"]

            # Posición
            try:
                lat = float(data["Posicion"][0])
                lon = float(data["Posicion"][1])
            except Exception:
                lat = "Unknown"
                lon = "Unknown"

            # Individuos (I_individuo es un array_list)
            individuos = data.get("I_individuo", None)
            if individuos is not None:
                num_individuos = al.size(individuos)

                # primeros 3
                j = 0
                while j < num_individuos and j < 3:
                    tags_primeros.append(al.get_element(individuos, j))
                    j += 1

                # últimos 3
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

        # distancia al siguiente vértice
        if i == num_vertices - 1:
            dist_sig = 0.0
        else:
            u = vid
            v = camino[i + 1]
            edge = G.get_edge(graph, u, v)
            if edge is not None:
                try:
                    dist_sig = float(E.weight(edge))
                except Exception:
                    dist_sig = 0.0
            else:
                dist_sig = 0.0

        info_v = {
            "id": vid,
            "latitud": lat,
            "longitud": lon,
            "num_individuos": num_individuos,
            "tags_primeros": tags_primeros,
            "tags_ultimos": tags_ultimos,
            "dist_siguiente_km": dist_sig
        }
        resumen_vertices.append(info_v)
        i += 1

    # 8. Primeros 5 y últimos 5 vértices
    if num_vertices > 5:
        primeros_5 = resumen_vertices[:5]
        ultimos_5 = resumen_vertices[-5:]
    else:
        primeros_5 = resumen_vertices[:]
        ultimos_5 = resumen_vertices[:]

    # 9. Completar resultado
    resultado["hay_camino"] = True
    resultado["distancia_total_km"] = float(distancia_total)
    resultado["num_vertices_camino"] = num_vertices
    resultado["camino"] = camino
    resultado["resumen_vertices"] = resumen_vertices
    resultado["primeros_5"] = primeros_5
    resultado["ultimos_5"] = ultimos_5
    resultado["ultimo_dentro_radio"] = ultimo_dentro
    resultado["mensaje"] = "Camino encontrado correctamente con BFS."

    return resultado

def peso_haversine_req3(graph, a, b):
    vA = G.get_vertex(graph, a)["value"]["Posicion"]
    vB = G.get_vertex(graph, b)["value"]["Posicion"]
    return haversine(float(vA[0]), float(vA[1]), float(vB[0]), float(vB[1]))



def req_3(catalog):
    start = time.process_time()

    def Topological_Sort(graph):
        verts = G.vertices(graph)
        num_v = al.size(verts)
        dfo = dfo_structure.new_dfo_structure(num_v)

        for i in range(num_v):
            vid = al.get_element(verts, i)
            if m.get(dfo["marked"], vid) is None:
                DFO.dfs_modified(graph, vid, dfo)

        visitados = 0
        for i in range(num_v):
            vid = al.get_element(verts, i)
            if m.get(dfo["marked"], vid) is not None:
                visitados += 1

        if visitados < num_v:
            return None

        orden = al.new_list()
        while not stack.is_empty(dfo["reversepost"]):
            v = stack.pop(dfo["reversepost"])
            al.add_last(orden, v)

        return orden

    graph = catalog["movement"]
    ordenado = Topological_Sort(graph)
    if ordenado is None:
        return None

    ordenado_size = al.size(ordenado)
    if ordenado_size == 0:
        return None

    dist = {}
    parent = {}
    for i in range(ordenado_size):
        vertex = al.get_element(ordenado, i)
        dist[vertex] = 0.0
        parent[vertex] = None

    for i in range(ordenado_size):
        vertex = al.get_element(ordenado, i)
        adjacents = G.adjacents(graph, vertex)
        if adjacents is None:
            continue

        for j in range(al.size(adjacents)):
            adjV = al.get_element(adjacents, j)
            peso = peso_haversine_req3(graph, vertex, adjV)
            if dist[adjV] < dist[vertex] + peso:
                dist[adjV] = dist[vertex] + peso
                parent[adjV] = vertex

    endV = None
    max_dist = -1.0
    for v in dist:
        if dist[v] > max_dist:
            max_dist = dist[v]
            endV = v
    if endV is None:
        return None

    camino = al.new_list()
    visitados_camino = set()
    current = endV
    while current is not None and current not in visitados_camino:
        visitados_camino.add(current)
        al.add_first(camino, current)
        current = parent[current]

    totV = al.size(camino)
    if totV == 0:
        return None

    individuos_set = set()
    for i in range(totV):
        vid = al.get_element(camino, i)
        vertex = G.get_vertex(graph, vid)
        individuos = vertex["value"]["I_individuo"]
        for k in range(al.size(individuos)):
            individuos_set.add(al.get_element(individuos, k))
    totIndiv = len(individuos_set)

    def formatealo(idx):
        vid = al.get_element(camino, idx)
        vertex = G.get_vertex(graph, vid)
        val = vertex["value"]
        primeros = []
        ultimos = []

        lat, lon = val["Posicion"]
        if lat is None:
            lat = "Unknown"
        if lon is None:
            lon = "Unknown"

        individuos = val["I_individuo"]
        n_indiv = al.size(individuos)

        for i in range(min(3, n_indiv)):
            primeros.append(al.get_element(individuos, i))
        for i in range(max(0, n_indiv - 3), n_indiv):
            ultimos.append(al.get_element(individuos, i))

        dist1 = None
        if idx > 0:
            prev_id = al.get_element(camino, idx - 1)
            peso = peso_haversine_req3(graph, prev_id, vid)
            dist1 = peso if peso is not None else "Unknown"

        dist2 = None
        if idx < totV - 1:
            next_id = al.get_element(camino, idx + 1)
            peso = peso_haversine_req3(graph, vid, next_id)
            dist2 = peso if peso is not None else "Unknown"

        return {
            "punto_id": vid,
            "latitud": lat,
            "longitud": lon,
            "num_individuos": n_indiv,
            "primeros_3_individuos": primeros,
            "ultimos_3_individuos": ultimos,
            "distancia_anterior": dist1,
            "distancia_siguiente": dist2
        }

    primeros = []
    ultimos = []
    for i in range(min(5, totV)):
        primeros.append(formatealo(i))
    for i in range(max(0, totV - 5), totV):
        ultimos.append(formatealo(i))

    end = time.process_time()
    elapsed = delta_time(start, end)

    return {
        "total_puntos": totV,
        "total_individuos": totIndiv,
        "primeros_5": primeros,
        "ultimos_5": ultimos,
        "tiempo_ms": elapsed
    }
    

#ANEXA A REQ 4

def format_vertex_r4(graph, vid):
    v = G.get_vertex(graph, vid)
    val = v["value"]

    lat, lon = val["Posicion"]
    individuos = val["I_individuo"]
    total = al.size(individuos)

    primeros = []
    ultimos = []

    for i in range(min(3, total)):
        primeros.append(al.get_element(individuos, i))

    for i in range(max(0, total - 3), total):
        ultimos.append(al.get_element(individuos, i))

    return {
        "punto_id": vid,
        "latitud": lat,
        "longitud": lon,
        "num_individuos": total,
        "primeros_3_individuos": primeros,
        "ultimos_3_individuos": ultimos
    }   

def req_4(catalog, lat_o, lon_o):
    """
    Retorna el resultado del requerimiento 4
    """

    start = get_time()

    graph = catalog["water"]

    # 1. nodo origen más cercano  (OJO: devuelve (id, dist))
    origen_id, _ = nearest_vertex_by_coord(catalog, lat_o, lon_o)

    if origen_id is None:
        end = get_time()
        return {
            "mensaje": "No se encontró un punto migratorio cercano al origen.",
            "tiempo_ms": delta_time(start, end)
        }

    # 2. correr Prim desde ese nodo
    res = prim_mst(graph, origen_id)
    if res is None:
        end = get_time()
        return {
            "mensaje": "No existe red hídrica viable desde el punto dado",
            "tiempo_ms": delta_time(start, end)
        }

    parent, dist, visited = res

    # 3. calcular puntos, individuos y distancia total (peso total del MST)
    total_pts = len(visited)
    distancia_total = 0.0
    individuos_set = set()

    for v in visited:
        d = dist[v]
        if d != float("inf"):
            distancia_total += d

        vertex = G.get_vertex(graph, v)
        lista = vertex["value"]["I_individuo"]

        for i in range(al.size(lista)):
            individuos_set.add(al.get_element(lista, i))

    total_individuos = len(individuos_set)

    # 4. convertir visited en lista ordenada para mostrar primeros/últimos
    lista_vertices = list(visited)
    lista_vertices.sort()  # orden alfabético para consistencia

    primeros = []
    ultimos = []

    for i in range(min(5, len(lista_vertices))):
        primeros.append(format_vertex_r4(graph, lista_vertices[i]))

    for i in range(max(0, len(lista_vertices) - 5), len(lista_vertices)):
        ultimos.append(format_vertex_r4(graph, lista_vertices[i]))

    end = get_time()

    return {
        "total_puntos": total_pts,
        "total_individuos": total_individuos,
        "distancia_total": distancia_total,
        "primeros_5": primeros,
        "ultimos_5": ultimos,
        "tiempo_ms": delta_time(start, end)
    }


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
    start = time.process_time()

    graph = catalog["water"]

    vertices = G.vertices(graph)
    n_vertices = al.size(vertices)

    # construir vecinos como grafo NO dirigido
    neighbors = {}
    for i in range(n_vertices):
        u = al.get_element(vertices, i)
        neighbors[u] = set()

    for i in range(n_vertices):
        u = al.get_element(vertices, i)
        adj = G.adjacents(graph, u)
        if adj is None:
            continue
        for j in range(al.size(adj)):
            v = al.get_element(adj, j)
            neighbors[u].add(v)
            if v not in neighbors:
                neighbors[v] = set()
            neighbors[v].add(u)

    visitados = set()
    subredes = []
    subred_id = 1

    for i in range(n_vertices):
        v_id = al.get_element(vertices, i)
        if v_id in visitados:
            continue

        # BFS en grafo NO dirigido
        cola = [v_id]
        visitados.add(v_id)
        comp_vertices = []

        while cola:
            u = cola.pop(0)
            comp_vertices.append(u)

            for w in neighbors.get(u, []):
                if w not in visitados:
                    visitados.add(w)
                    cola.append(w)

        if len(comp_vertices) == 0:
            continue

        lat_min = None
        lat_max = None
        lon_min = None
        lon_max = None

        individuos_set = set()
        puntos_info = []

        for vid in comp_vertices:
            vert = G.get_vertex(graph, vid)
            if vert is None:
                continue
            val = vert["value"]

            lat = val["Posicion"][0]
            lon = val["Posicion"][1]

            try:
                lat_f = float(lat)
                lon_f = float(lon)
                if lat_min is None or lat_f < lat_min:
                    lat_min = lat_f
                if lat_max is None or lat_f > lat_max:
                    lat_max = lat_f
                if lon_min is None or lon_f < lon_min:
                    lon_min = lon_f
                if lon_max is None or lon_f > lon_max:
                    lon_max = lon_f
            except Exception:
                pass

            ind_list = val.get("I_individuo", None)
            num_ind = 0
            if ind_list is not None:
                num_ind = al.size(ind_list)
                for k in range(num_ind):
                    individuos_set.add(al.get_element(ind_list, k))

            puntos_info.append({
                "id": vid,
                "lat": lat if lat is not None else "Unknown",
                "lon": lon if lon is not None else "Unknown",
                "num_individuos": num_ind,
                "ind_list": ind_list
            })

        num_puntos = len(puntos_info)
        num_individuos = len(individuos_set)

        puntos_info.sort(key=lambda x: str(x["id"]))

        primeros_puntos = []
        ultimos_puntos = []

        for idx in range(min(3, num_puntos)):
            p = puntos_info[idx]
            primeros_puntos.append({
                "id": p["id"],
                "latitud": p["lat"],
                "longitud": p["lon"]
            })

        for idx in range(max(0, num_puntos - 3), num_puntos):
            p = puntos_info[idx]
            ultimos_puntos.append({
                "id": p["id"],
                "latitud": p["lat"],
                "longitud": p["lon"]
            })

        individuos_ordenados = sorted(individuos_set, key=lambda x: str(x))
        n_ind = len(individuos_ordenados)

        primeros_ind = individuos_ordenados[:3] if n_ind > 0 else []
        ultimos_ind = individuos_ordenados[-3:] if n_ind > 0 else []

        subredes.append({
            "id_subred": subred_id,
            "num_puntos": num_puntos,
            "lat_min": lat_min if lat_min is not None else "Unknown",
            "lat_max": lat_max if lat_max is not None else "Unknown",
            "lon_min": lon_min if lon_min is not None else "Unknown",
            "lon_max": lon_max if lon_max is not None else "Unknown",
            "num_individuos": num_individuos,
            "primeros_puntos": primeros_puntos,
            "ultimos_puntos": ultimos_puntos,
            "primeros_individuos": primeros_ind,
            "ultimos_individuos": ultimos_ind
        })

        subred_id += 1

    total_subredes = len(subredes)

    if total_subredes == 0:
        end = time.process_time()
        return {
            "total_subredes": 0,
            "subredes_top": [],
            "tiempo_ms": delta_time(start, end)
        }

    subredes.sort(key=lambda s: (-s["num_puntos"], s["id_subred"]))
    top_subredes = subredes[:5]

    end = time.process_time()
    elapsed = delta_time(start, end)

    return {
        "total_subredes": total_subredes,
        "subredes_top": top_subredes,
        "tiempo_ms": elapsed
    }



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
