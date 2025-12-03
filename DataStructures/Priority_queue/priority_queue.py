"""
    RECORDATORIO
    Para un nodo i
    Su padre es i//2
    sus hijos son 2*i y 2*i+1 (izq y der)
"""
from DataStructures.Priority_queue import pq_entry as pq


def new_heap(is_min_pq=True):
    if is_min_pq == True:
        cmp_function = default_compare_lower_value
    else:
        cmp_function = default_compare_higher_value
    
    my_heap = { "elements":{"elements":[None], "size":1},
               "size":0,
               "cmp_function":cmp_function
    }
    return my_heap

def default_compare_lower_value(father_node, child_node):
    if pq.get_priority(father_node) >= pq.get_priority(child_node):
        return True
    return False

def default_compare_higher_value(father_node, child_node):
    if pq.get_priority(father_node) >= pq.get_priority(child_node):
        return True
    return False

def insert(my_heap, priority, value):
    current = pq.new_pq_entry(priority, value)
    
    my_heap["elements"]["elements"].append(current)
    my_heap["size"] += 1
    swim(my_heap, my_heap["size"])
    return my_heap

def swim(my_heap, pos):
    elements = my_heap["elements"]["elements"]
    cmp = my_heap["cmp_function"]
      
    while pos >1:
        Parent = pos // 2
        if cmp(elements[pos],elements[Parent]): #asegura que la función no inicie en padre
            exchange(my_heap,Parent,pos)
            pos = Parent
        else:
            pos = 1 #
    return None

def priority(my_heap, parent, child):
    cmp = my_heap["cmp_function"]
    res = cmp(child,parent)
    return res

def exchange(my_heap, father_node, child_node): #Asumo me devuelve posición (De acuerdo con swim)
    elements = my_heap["elements"]["elements"]
    
    hldr1 = elements[father_node]
    hldr2 = elements[child_node]
    elements[father_node] = hldr2
    elements[child_node] = hldr1
    return my_heap

def is_empty(my_heap):
    if size(my_heap) == 0:
        return True
    else:
        return False

def size(my_heap):
    res = 0
    elements = my_heap["elements"]["elements"]
    
    for i in range (1,len(elements)):
        if elements[i] != None:
            res +=1
    return res

def sink(my_heap, pos):
    elementos = my_heap["elements"]["elements"]
    n = len(elementos) - 1
    cmp = my_heap["cmp_function"]

    continuar = True
    while continuar == True:
        hijo_izq = 2 * pos
        hijo_der = hijo_izq + 1
        mejor = pos

        if hijo_izq <= n and elementos[hijo_izq] is not None and cmp(elementos[hijo_izq], elementos[mejor]):
            mejor = hijo_izq
        if hijo_der <= n and elementos[hijo_der] is not None and cmp(elementos[hijo_der], elementos[mejor]):
            mejor = hijo_der

        if mejor != pos:
            exchange(my_heap, pos, mejor)
            pos = mejor
        else:
            continuar = False
    return None


def remove(my_heap):
    if is_empty(my_heap):
        return None

    elementos = my_heap["elements"]["elements"]
    valor_tope = pq.get_value(elementos[1])

    ultimo = len(elementos) - 1
    if ultimo == 1:
        elementos.pop()
    else:
        elementos[1] = elementos[ultimo]
        elementos.pop()
        sink(my_heap, 1)

    my_heap["size"] = size(my_heap)
    return valor_tope


def get_first_priority(my_heap):
    if is_empty(my_heap):
        return None
    elementos = my_heap["elements"]["elements"]
    if len(elementos) > 1 and elementos[1] is not None:
        return pq.get_value(elementos[1])
    return None


def is_present_value(my_heap, value):
    elementos = my_heap["elements"]["elements"]
    i = 1
    posicion = -1
    while i < len(elementos) and posicion == -1:
        nodo = elementos[i]
        if nodo is not None and pq.get_value(nodo) == value:
            posicion = i
        i = i + 1
    return posicion


def contains(my_heap, value):
    existe = False
    if is_present_value(my_heap, value) != -1:
        existe = True
    return existe

#En la documentacion de esta funcion se describe basicamente lo mismo que la funcion anterior xd, lit la diferencia es que una da la posicion y la otra un booleano


def improve_priority(my_heap, priority, value):
    elementos = my_heap["elements"]["elements"]
    idx = is_present_value(my_heap, value)

    if idx == -1:
        return my_heap

    if hasattr(pq, "set_priority"):
        pq.set_priority(elementos[idx], priority)
    else:
        val = pq.get_value(elementos[idx])
        elementos[idx] = pq.new_pq_entry(priority, val)

    
    hubo_ajuste = True
    while hubo_ajuste == True:
        swim(my_heap, idx)
        sink(my_heap, idx)
        hubo_ajuste = False

    my_heap["size"] = size(my_heap)
    return my_heap
