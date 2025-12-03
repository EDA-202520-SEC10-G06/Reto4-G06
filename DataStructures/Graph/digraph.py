from DataStructures.Map import map_linear_probing as mlp
from DataStructures.Map import map_entry as me
from DataStructures.List import array_list as ar

def new_graph(order):
    vertices = mlp.new_map(order,0.5,109345121)
    my_graph = {"vertices":vertices,"num_edges":0}
    return my_graph

def insert_vertex(my_graph, key_u, info_u):
    #G.insert_vertex(my_graph, "Armenia", {"nombre": "Armenia", "población": 300000})
    
    #La key es armenia
    #Value es otro map_entry
    #Key de Value es la misma key (armenia)
    #Value de Value son los valores "nombre" , "población"
    #adjacents de value es un nuevo mapa???????????????????????
    
    # Y si ya existía se actualiza
    vertex = {"key": key_u,
              "value": info_u,
              "adjacents": mlp.new_map(1,0.5)}
    
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
    
    #G.add_edge(my_graph, "Armenia", "Bogota", 100)
    #En el grafo, en vertices, en el vértice de llave Armenia, en los adyacentes:
    #Se añade en formato map_entry (key, value), la llave bogotá y un value
    #Value de Bogota contiene: {"to": "Bogota", "weight":100}
    #carajo
    
    hldr1 = mlp.get(my_graph["vertices", key_u])
    if hldr1 == None:
        raise Exception("El vértice '"+str(key_u)+"' No existe en el grafo.")
    
    hldr2 = mlp.get(my_graph["vertices", key_v])
    if hldr2 == None:
        raise Exception("El vértice '"+str(key_v)+"' No existe en el grafo.")
    
    adjacents = hldr1["value"]["adjacents"]
    arco = mlp.get(adjacents,key_v)
    
    if arco == None:
        info = {"vertex": key_v,
                "weight":weight}
        mlp.put(adjacents,key_v,info)
        my_graph["num_edges"]+=1
    else:
        info = arco["value"]
        info["weight"] = weight
        mlp.put(adjacents,key_v,info)
        
    
    
def contains_vertex(my_graph, key_u):
    #True o false dependiendo si lo contiene o no
    hldr = mlp.get(my_graph["vertices"], key_u)
    if hldr == None:
        return False
    return True

def order(my_graph):
    #cuantos vertices hay (reccorrido de grafo["vertices"]["table"]["elements"] donde se cuentan aquellos que sssean diferentes a None)
    #["vertices"]["size"]
    
    return my_graph["vertices"]["size"]

def size(my_graph):
    #cuantos arcos hay (reccorrido de grafo["vertices"]["table"][i]["value"]["adjacents"]["table"]["elements"] donde se cuentan aquellos que sssean diferentes a None)
    #["num_edges"]
    #Gracias grafos por:
    return my_graph["num_edges"]

def degree(my_graph, key_u):
    #Retorna el grado del vertice con llave key_u, es decir, el numero de arcos adyacentes al vertice.
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex == None:
        raise Exception("El vértice '"+str(key_u)+"' No existe en el grafo.")
    
    adjacents = vertex["value"]["adjacents"]
    return adjacents["size"]

def adjacents(my_graph, key_u):
    #Retorna los adyacentes en forma de array
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex == None:
        raise Exception("El vértice '"+str(key_u)+"' No existe en el grafo.")
    
    hldr = vertex["value"]["adjacents"]["table"]["elements"]
    res = ar.new_list()
    for node in hldr:
        if node["key"] != None:
            ar.add_last(res,node["key"])
    
    return res

def vertices(my_graph):
    #Retorna los vértices en forma de array
    hldr = my_graph["vertices"]["table"]["elements"]
    res = ar.new_list()
    
    for node in hldr:
        if node["key"] != None:
            ar.add_last(res,node["key"])
    
    return res

def edges_vertex(my_graph, key_u):
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex == None:
        raise Exception("El vértice '"+str(key_u)+"' No existe en el grafo.")
    
    hldr = vertex["value"]["adjacents"]["table"]["elements"]
    res = ar.new_list()
    
    for node in hldr:
        if node["key"] != None:
            ar.add_last(res,node["value"])
    
    return res


def get_vertex(my_graph, key_u):
    #Si no hay vertice, el get retorna None :VVVV
    return mlp.get(my_graph["vertices"], key_u)
    

def update_vertex_info(my_graph, key_u, new_info_u):
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex == None:
        return my_graph
    
    vertex["value"]["value"] = new_info_u
    return my_graph

def get_vertex_information(my_graph, key_u):
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex == None:
        return None
    return vertex["value"]["value"]