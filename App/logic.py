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

def haversine(lat_a, lon_a, lat_b, lon_b):
    R = 6371
    lat_a, lon_a, lat_b, lon_b = map(math.radians, [lat_a, lon_a, lat_b, lon_b])
    dlat = lat_b - lat_a
    dlon = lon_b - lon_a
    a = math.sin(dlat/2)*2 + math.cos(lat_a)*math.cos(lat_b)*math.sin(dlon/2)*2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

# Funciones de consulta sobre el catálogo


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


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
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

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
