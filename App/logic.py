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

# Funciones de consulta sobre el catálogo


def req_1(catalog, lat_o, lon_o, lat_d, lon_d, individuo):
    """
    Retorna el resultado del requerimiento 1
    """
    #TODO: Modificar el requerimiento 1

    start = get_time()


    graph = catalog["movement"]

    # 1. encontrar nodo origen y destino
    origen = get_closest_vertex(catalog, lat_o, lon_o)
    destino = get_closest_vertex(catalog, lat_d, lon_d)

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
    path = dfs_path(graph, origen, destino)
    if path is None:
        return {
            "primer_nodo_del_individuo": first_node,
            "mensaje": "No existe un camino viable entre los puntos"
        }

    total_pts = al.size(path)

    # 4. calcular distancia total
    dist_total = 0.0
    for i in range(total_pts - 1):
        A = al.get_element(path, i)
        B = al.get_element(path, i+1)
        e = G.get_edge(graph, A, B)
        if e:
            dist_total += e["weight"]

    # 5. primeros 5 y últimos 5
    primeros = []
    ultimos = []

    for i in range(min(5, total_pts)):
        vid = al.get_element(path, i)
        next_vid = al.get_element(path, i+1) if i+1 < total_pts else None
        primeros.append(format_vertex(graph, vid, next_vid))

    for i in range(max(0, total_pts-5), total_pts):
        vid = al.get_element(path, i)
        next_vid = al.get_element(path, i+1) if i+1 < total_pts else None
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


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    start = time.process_time()
    
    #Definición de la función de restricción 💔🥀
    def Topological_Sort(graph):
        #Defino variables que necesito
        dfo = DFO.dfs_modified(graph)
        num_v = al.size(G.vertices(graph))
        visitados = 0
        marked = dfo["marked"]
        keys = m.key_set(marked)
        
        
        #Recorro las llaves
        for i in range(al.size(keys)):
            if m.get(marked, al.get_element(keys,i)) != None:
                visitados +=1
        
        #Verifico que logró recorrer todo (que sea un DAG)
        if visitados < num_v:
            return None
        
        #Genera una lista que retornará el resultado del sort
        res = al.new_list()
        
        #Recorro haciendo pop al reversepost (que ya en sí es el resultado pero alrevez)
        while not stack.is_empty(dfo["reversepost"]):
            v = stack.pop(dfo["reversepost"])
            al.add_last(res,v)

        return res
    
    #Código real del requerimiento 3
    
    #Ordeno los datos y reviso que hayan suficientes para ejecutar
    graph = catalog["movement"]
    ordenado = Topological_Sort(graph)
    ordenado_size = al.size(ordenado)
    if ordenado == None or ordenado_size == 0:
        return None
    
    #Creo holders que van a contener la info solicitada (dist contiene vértices, parent contiene el vértice anterior al actual de dist)
    dist = {}
    parent = {}
    #Recorro el ordenado y meto los datos en la estructura
    for i in range(ordenado_size):
        vertex = al.get_element(ordenado, i)
        dist[vertex] = 0
        parent[vertex] = None
    
    #Por cada vértice que tengam reviso los adyacentes para calcular las rutas más frecuentada (para definir ruta migratoria)
    for i in range(ordenado_size):
        vertex = al.get_element(ordenado, i)
        adjacents = G.adjacents(graph, vertex)
        
        for j in range(al.size(adjacents)):
            adjV = al.get_element(adjacents,j)
            peso = peso_haversine(graph, vertex, adjV)
            
            if dist[adjV] < dist[vertex] + peso:
                dist[adjV] = dist[vertex] + peso
                parent[adjV] = vertex
    
    #Busca el vértice final entre los ya seleccionados
    endV = None
    max_dist = -1
    
    for v in dist:
        if dist[v] > max_dist:
            max_dist = dist[v]
            endV = v
    
    if endV == None:
        return None

    #Con el vértice final y los vértices más frecuentados puedo construir la ruta migratoria
    camino = al.new_list()
    current = endV
    
    while current != None:
        al.add_first(camino, current)
        current = parent[current]
    
    totV = al.size(camino)
    if totV == 0:
        return None
    
    #Calculo el total de individuos que utilizan la ruta migratoria
    individuos_set= set()
    for i in range(totV):
        vid = al.get_element(camino, i)
        vertex = G.get_vertex(graph, vid)
        individuos = vertex["value"]["I_individuo"]
        
        for k in range(al.size(individuos)):
            individuos_set.add(al.get_element(individuos, k))
    
    totIndiv = len(individuos_set)
    
    #Función para formatear datos del vértice
    def formatealo(idx):
        #Obtengo los datos
        vid = al.get_element(camino, idx)
        vertex=G.get_vertex(graph, vid)
        val = vertex["value"]
        primeros = []
        ultimos = []
        
        lat, lon = val["Posicion"]
        if lat == None:
            lat = "Unknown"
        if lon == None:
            lon = "Unknown"
        
        individuos = val["I_individuo"]
        totIndiv = al.size(individuos)
        
        #Comienso a formatear:
        for i in range (min(3, totIndiv)):
            primeros.append(al.get_element(individuos, i))
        
        for i in range (max(0, totIndiv-3), totIndiv):
            ultimos.append(al.get_element(individuos, i))
        
        #Distancia al anterior
        dist1 = None
        if idx>0:
            prev_id = al.get_element(camino, idx - 1)
            peso = peso_haversine(graph, prev_id, vid)
            dist1 = peso if peso!=None else "Unknown"
        
        #Distancia al proximo
        dist2 = None
        if idx < totV-1:
            next_id = al.get_element(camino, idx + 1)
            peso = peso_haversine(graph, vid, next_id)
            dist2 = peso if peso!=None else "Unknown"
        
        return{
            "punto_id": vid,
            "latitud": lat,
            "longitud": lon,
            "num_individuos": totIndiv,
            "primeros_3_individuos": primeros,
            "ultimos_3_individuos": ultimos,
            "distancia_anterior": dist1,
            "distancia_siguiente": dist2
        }
    
    primeros=[]
    ultimos=[]
    for i in range(min(5, totV)):
        primeros.append(formatealo(i))
    for i in range(max(0, totV-5),totV):
        ultimos.append(formatealo(i))
    
    end = time.process_time()
    elapsed = delta_time(start,end)
    
    return{"total_puntos": totV,
           "total_individuos": totIndiv,
           "primeros_5": primeros,
           "ultimos_5": ultimos,
           "time":elapsed}
    

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
    Retorna el resultado del requerimiento 5
    """
    #TODO: Modificar el requerimiento 4

    start = get_time()

    graph = catalog["water"]

    # 1. nodo origen más cercano
    origen = get_closest_vertex(catalog, lat_o, lon_o)

    # 2. correr Prim desde ese nodo
    res = prim_mst(graph, origen)
    if res is None:
        return {"mensaje": "No existe red hídrica viable desde el punto dado"}

    parent, dist, visited = res

    # 3. calcular puntos, individuos y distancia total
    total_pts = len(visited)
    distancia_total = 0
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
