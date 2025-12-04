
from DataStructures.Graph import digraph as G
from DataStructures.Graph import edge as E
from DataStructures.Graph import dijsktra_structure as dstruct

from DataStructures.Map import map_linear_probing as mp
from DataStructures.List import array_list as lt
from DataStructures.Stack import stack as st


def dijkstra(my_graph, source):
    """
    Implementa el algoritmo de Dijkstra para encontrar los caminos
    de costo mínimo desde el vértice `source` hacia todos los vértices
    alcanzables en el grafo `my_graph`.

    Retorna una estructura de tipo dijsktra_structure:

        {
            "source": source,
            "visited": mapa key -> {
                "edge_from": vértice anterior en el camino mínimo (o None),
                "dist_to":  distancia acumulada desde source,
                "marked":   True si ya fue procesado definitivamente
            }
        }
    """

    # Estructura base (ya contiene el mapa visited y la pq, aunque aquí no usamos la pq)
    aux_structure = dstruct.new_dijsktra_structure(source, G.order(my_graph))
    aux_structure["source"] = source
    visited = aux_structure["visited"]

    # Inicializar la info del vértice fuente
    info_source = {
        "edge_from": None,
        "dist_to": 0.0,
        "marked": False
    }
    mp.put(visited, source, info_source)

    # Lista de vértices “en frontera” (los que ya conocemos pero aún no están marcados)
    frontier = lt.new_list()
    lt.add_last(frontier, source)

    # Bucle principal de Dijkstra
    while True:
        # 1. Escoger el vértice no marcado con menor distancia en la frontera
        min_vertex = None
        min_dist = float("inf")

        n_frontier = lt.size(frontier)
        for i in range(n_frontier):
            key_v = lt.get_element(frontier, i)
            info_v = mp.get(visited, key_v)
            if (not info_v["marked"]) and info_v["dist_to"] < min_dist:
                min_dist = info_v["dist_to"]
                min_vertex = key_v

        # Si no encontramos vértice candidato, terminamos
        if min_vertex is None:
            break

        # 2. Marcar el vértice seleccionado
        info_min = mp.get(visited, min_vertex)
        info_min["marked"] = True
        mp.put(visited, min_vertex, info_min)

        # 3. Relajar aristas salientes de min_vertex
        adj_list = G.adjacents(my_graph, min_vertex)  
        deg = lt.size(adj_list)

        for i in range(deg):
            key_w = lt.get_element(adj_list, i)

            edge_vw = G.get_edge(my_graph, min_vertex, key_w)
            if edge_vw is None:
                # si por alguna razón no está el arco, lo saltamos
                continue

            weight_vw = E.weight(edge_vw)

            new_dist = info_min["dist_to"] + weight_vw

            if not mp.contains(visited, key_w):
                # Primer vez que vemos a key_w
                info_w = {
                    "edge_from": min_vertex,
                    "dist_to": new_dist,
                    "marked": False
                }
                mp.put(visited, key_w, info_w)
                lt.add_last(frontier, key_w)
            else:
                # Ya lo conocíamos; revisar si mejoró el camino
                info_w = mp.get(visited, key_w)
                if new_dist < info_w["dist_to"]:
                    info_w["dist_to"] = new_dist
                    info_w["edge_from"] = min_vertex
                    mp.put(visited, key_w, info_w)

    return aux_structure


def dist_to(key_v, aux_structure):
    """
    Retorna la distancia mínima desde `source` hasta `key_v`
    usando la estructura resultante de `dijkstra`.

    Si no existe camino, retorna None.
    """
    visited = aux_structure["visited"]

    if not mp.contains(visited, key_v):
        return None

    info_v = mp.get(visited, key_v)
    return info_v["dist_to"]


def has_path_to(key_v, aux_structure):
    """
    Indica si existe un camino entre el vértice `source`
    (almacenado en aux_structure["source"]) y el vértice `key_v`.
    """
    visited = aux_structure["visited"]
    return mp.contains(visited, key_v)


def path_to(key_v, aux_structure):
    """
    Retorna una pila (stack) con el camino desde `source` hasta `key_v`.
    Al hacer pop sobre la pila, se obtienen los vértices en orden
    desde el origen hasta el destino.

    Si no existe camino, retorna None.
    """
    if not has_path_to(key_v, aux_structure):
        return None

    visited = aux_structure["visited"]
    source = aux_structure["source"]

    path_stack = st.new_stack()
    current = key_v

    
    while True:
        st.push(path_stack, current)
        info_curr = mp.get(visited, current)
        prev = info_curr["edge_from"]
        if prev is None:
            break
        current = prev

    
    return path_stack