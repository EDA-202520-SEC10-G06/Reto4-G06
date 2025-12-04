from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as st
from DataStructures.Map import map_linear_probing as map
from DataStructures.Graph import digraph as G
from DataStructures.List import array_list as lt


def bfs(my_graph, source):
    """
    Inicia el recorrido BFS desde 'source'.
    Crea visited_map con:
      edge_from: predecesor
      dist_to: distancia desde source
    """
    visited = map.new_map(
        num_elements=G.order(my_graph),
        load_factor=0.5
    )

    # Información del vértice fuente
    map.put(visited, source, {
        "edge_from": None,
        "dist_to": 0
    })

    return bfs_vertex(my_graph, source, visited)


def bfs_vertex(my_graph, source, visited):
    """
    Recorre el grafo usando BFS desde 'source'.
    visited almacena distancias y predecesores.
    """
    queue = q.new_queue()
    q.enqueue(queue, source)

    while not q.is_empty(queue):
        current = q.dequeue(queue)

        
        adj_list = G.adjacents(my_graph, current)
        if adj_list is None:
            continue

        current_info = map.get(visited, current)
        current_dist = current_info["dist_to"]

        
        for i in range(lt.size(adj_list)):
            neighbor = lt.get_element(adj_list, i)

            
            if not map.contains(visited, neighbor):
                map.put(visited, neighbor, {
                    "edge_from": current,
                    "dist_to": current_dist + 1
                })
                q.enqueue(queue, neighbor)

    return visited


def has_path_to(key_v, visited):
    """Retorna True si 'key_v' fue alcanzado por BFS."""
    return map.contains(visited, key_v)


def path_to(key_v, visited):
    """
    Reconstruye el camino usando los edge_from en visited.
    Retorna una pila stack con el camino desde source hasta key_v.
    """
    if not has_path_to(key_v, visited):
        return None

    path = st.new_stack()
    current = key_v

    while True:
        st.push(path, current)
        info = map.get(visited, current)
        prev = info["edge_from"]

        if prev is None:
            break
        current = prev

    return path
