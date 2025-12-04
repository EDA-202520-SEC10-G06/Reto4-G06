from DataStructures.Map import map_linear_probing as mlp
from DataStructures.Map import map_entry as me
from DataStructures.List import array_list as ar
from DataStructures.Graph import edge as E


def new_graph(order):
    vertices = mlp.new_map(order, 0.5, 109345121)
    my_graph = {"vertices": vertices, "num_edges": 0}
    return my_graph


def insert_vertex(my_graph, key_u, info_u):
    """
    Inserta un vértice en el grafo.

    Estructura del vértice:
    {
        "key": key_u,
        "value": info_u,              # info asociada (la que tú le mandas desde logic.py)
        "adjacents": map()            # mapa de adyacentes key_v -> info_arco
    }

    info_arco:
    {
        "vertex": key_v,
        "weight": peso
    }
    """
    vertex = {
        "key": key_u,
        "value": info_u,
        "adjacents": mlp.new_map(1, 0.5)
    }

    mlp.put(my_graph["vertices"], key_u, vertex)
    return my_graph


def add_edge(my_graph, key_u, key_v, weight=1.0):
    """
    Se busca el vertice key_u en el grafo y se verifica si existe. 
    Si no existe, se lanza una excepcion. 
    Se busca el vertice key_v en el grafo y se verifica si existe. 
    Si no existe, se lanza una excepcion. 
    Si ambos vertices existen y el arco NO existe, se agrega el arco de key_u a key_v. 
    Si el arco ya existe, se reemplaza el peso del arco (NO se aceptan arcos paralelos).
    """

    # Obtener el mapa de vértices
    vertices = my_graph["vertices"]

    # Buscar vértices origen y destino
    hldr1 = mlp.get(vertices, key_u)
    if hldr1 is None:
        raise Exception("El vértice '" + str(key_u) + "' no existe en el grafo.")
    
    hldr2 = mlp.get(vertices, key_v)
    if hldr2 is None:
        raise Exception("El vértice '" + str(key_v) + "' no existe en el grafo.")

    # Mapa de adyacentes del vértice origen
    adjacents = hldr1["adjacents"]
    arco = mlp.get(adjacents, key_v)
    
    if arco is None:
        info = {
            "vertex": key_v,
            "weight": weight
        }
        mlp.put(adjacents, key_v, info)
        my_graph["num_edges"] += 1
    else:
        info = arco["value"]
        info["weight"] = weight
        mlp.put(adjacents, key_v, info)


def contains_vertex(my_graph, key_u):
    # True o False dependiendo si lo contiene o no
    hldr = mlp.get(my_graph["vertices"], key_u)
    if hldr is None:
        return False
    return True


def order(my_graph):
    # número de vértices
    return my_graph["vertices"]["size"]


def size(my_graph):
    # número de arcos
    return my_graph["num_edges"]


def degree(my_graph, key_u):
    """
    Retorna el grado del vertice con llave key_u,
    es decir, el numero de arcos adyacentes al vertice.
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        raise Exception("El vértice '" + str(key_u) + "' no existe en el grafo.")
    
    adjacents = vertex["adjacents"]
    return adjacents["size"]


def adjacents(my_graph, key_u):
    """
    Retorna los vértices adyacentes a key_u en forma de array_list
    con las LLAVES (los ids de los vértices vecinos).
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        raise Exception("El vértice '" + str(key_u) + "' no existe en el grafo.")
    
    hldr = vertex["adjacents"]["table"]["elements"]
    res = ar.new_list()
    for node in hldr:
        if node["key"] is not None:
            # node["key"] es la llave del vecino
            ar.add_last(res, node["key"])
    
    return res


def vertices(my_graph):
    """
    Retorna los vértices del grafo en forma de array_list
    con las LLAVES (ids de vértices).
    """
    hldr = my_graph["vertices"]["table"]["elements"]
    res = ar.new_list()
    
    for node in hldr:
        if node["key"] is not None:
            ar.add_last(res, node["key"])
    
    return res


def edges_vertex(my_graph, key_u):
    """
    Retorna los arcos salientes del vértice key_u
    en forma de array_list de diccionarios:
        {"vertex": key_v, "weight": peso}
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        raise Exception("El vértice '" + str(key_u) + "' no existe en el grafo.")
    
    hldr = vertex["adjacents"]["table"]["elements"]
    res = ar.new_list()
    
    for node in hldr:
        if node["key"] is not None:
            ar.add_last(res, node["value"])
    
    return res


def get_vertex(my_graph, key_u):
    # Si no hay vertice, el get retorna None
    return mlp.get(my_graph["vertices"], key_u)


def update_vertex_info(my_graph, key_u, new_info_u):
    """
    Actualiza la info almacenada en vertex["value"] (tu estructura de info_u).
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        return my_graph
    
    vertex["value"] = new_info_u
    return my_graph


def get_vertex_information(my_graph, key_u):
    """
    Retorna solo la parte 'value' del vértice (tu info_u).
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        return None
    return vertex["value"]


def get_edge(my_graph, key_u, key_v):
    """
    Retorna un arco estándar (edge) entre key_u y key_v si existe.
    Si no existe, retorna None.
    Es tolerante a las distintas formas en que pueda estar guardado el vértice.
    """

    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        return None

    # Buscar el mapa de adyacencias en las formas más comunes
    adjacents = None

    # Caso 1: el vértice tiene 'adjacents' al tope
    if isinstance(vertex, dict) and "adjacents" in vertex:
        adjacents = vertex["adjacents"]

    # Caso 2: el vértice está dentro de 'value'
    elif isinstance(vertex, dict) and "value" in vertex:
        val = vertex["value"]
        if isinstance(val, dict) and "adjacents" in val:
            adjacents = val["adjacents"]

    # Si no encontramos mapa de adyacencias, no hay arco
    if adjacents is None:
        return None

    arco = mlp.get(adjacents, key_v)
    if arco is None:
        return None

    # arco puede ser:
    #  - directamente {"vertex": ..., "weight": ...}
    #  - o un map_entry {"key": ..., "value": {...}}
    if isinstance(arco, dict) and "vertex" in arco and "weight" in arco:
        info = arco
    elif isinstance(arco, dict) and "value" in arco:
        info = arco["value"]
    else:
        # último caso: asumimos que 'arco' es solo el peso
        info = {"vertex": key_v, "weight": arco}

    return E.new_edge(info["vertex"], info["weight"])

def num_vertices(my_graph):
    """
    Alias de order(my_graph).
    Retorna el número de vértices del grafo.
    """
    return order(my_graph)


def num_edges(my_graph):
    """
    Alias de size(my_graph).
    Retorna el número de arcos del grafo.
    """
    return size(my_graph)