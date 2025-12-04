from DataStructures.Graph import dfo_structure as ds
from DataStructures.Graph import dfs
from DataStructures.Graph import digraph as G
from DataStructures.List import array_list as al
from DataStructures.Map import map_linear_probing as M
from DataStructures.Queue import queue as Q
from DataStructures.Stack import stack as S

def dfs_modified(catalog, vertex, dfo):
    M.put(dfo["marked"], vertex, True)
    Q.enqueue(dfo["pre"], vertex)
    
    adjacents = G.adjacents(catalog, vertex)
    for i in range(al.size(adjacents)):
        hldr = al.get_element(adjacents, i)
        if M.get(dfo["marked"], hldr) == None:
            dfs_modified(catalog, hldr, dfo)
    
    Q.enqueue(dfo["post"],vertex)
    S.push(dfo["reversepost"], vertex)

def DFO(catalog):
    vertices = G.vertices(catalog)
    num_V = al.size(vertices)
    dfo = ds.new_dfo_structure(num_V)
    
    for i in range(num_V):
        hldr = al.get_element(vertices, i)
        if M.get(dfo["marked"], hldr) == None:
            dfs(catalog, hldr, dfo)
            
    return dfo

