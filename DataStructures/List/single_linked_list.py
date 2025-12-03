def new_list():
    newlist = {
        "first":None,
        "last":None,
        "size":0
    }
    return newlist

def get_element(my_list, pos):
    searchpos = 0
    node = my_list["first"]
    while searchpos < pos:
        node = node["next"]
        searchpos += 1
    return node["info"]

def is_present(my_list, element, cmp_function):
    is_in_array = False
    temp = my_list["first"]
    count = 0
    while not is_in_array and temp is not None:
        if cmp_function(element,temp["info"]) == 0:
            is_in_array = True
        else:
            temp = temp["next"]
            count +=1
    if not is_in_array:
        count = -1
    return count


'''
#Recordatorio de estructura de nodos:
node = {info:None,
        next:None,
}
'''

def add_first(my_list, node):
    if type(node) != dict:
        node = {"info":node,"next":None}
    
    if my_list["first"] == None:
        my_list["first"]= node
        my_list["last"] = node
        my_list["size"] += 1
    else:
        node["next"] = my_list["first"]["next"]
        my_list["first"]= node
        my_list["size"] += 1
    return my_list

def add_last(my_list, node):
    if type(node) != dict:
        node = {"info":node,"next":None}
    
    if my_list["first"] == None:
        my_list["first"]= node
        my_list["last"] = node
        my_list["size"] += 1
    elif my_list["first"]["next"] == None:
        my_list["first"]["next"] = node
        my_list["last"] = node
        my_list["size"] += 1
    else:
        hldr = my_list["first"]
        while hldr["next"] != None:
            hldr = hldr["next"]
        hldr["next"] = node
        my_list["size"] += 1
    
    return my_list

def size(my_list):
    #ls = my_list["size"]
    hldr = 0
    temp = None
    if my_list["first"] != None:
        if my_list["first"]["next"] == None:
            hldr = 1
        else:
            temp = my_list["first"]
            while temp != None:
                temp = temp["next"]
                hldr +=1
                
    ls = hldr
    return ls

def first_element(my_list):
    node = my_list["first"]
    return node

def is_empty(my_list):
    """
    Verifica si la lista está vacía.

    Parameters:
        my_list (single_linked_list): Lista enlazada simple.

    Returns:
        bool: True si la lista está vacía, False en caso contrario.
    """
    size = my_list["size"]
    if size == 0: 
        return True 
    else: 
        return False
    

def last_element(my_list):
    """
    Retorna el último elemento de la lista.

    Parameters:
        my_list (single_linked_list): Lista enlazada simple.

    Returns:
        dict: Último nodo de la lista.
    """
    if is_empty(my_list):
        return None
    else:
        return my_list["last"]
    

def delete_element(my_list,pos):
    """
    Elimina el elemento en la posición dada.

    Parameters:
        my_list (single_linked_list): Lista enlazada simple.
        pos (int): Posición del elemento a eliminar.

    Returns:
        single_linked_list: Lista actualizada.
    """
    if pos < 0 or pos >= size(my_list):
        return my_list
    
    if pos == 0:
        to_delete = my_list["first"]
        my_list["first"] = to_delete["next"]
        new = to_delete["next"]
        if new == None:
            my_list["last"] = new
        my_list["size"] -= 1
    else: 
        searchpos = 0
        node = my_list["first"]
        while searchpos < pos:
            temp = node
            node = node["next"]
            searchpos += 1
        if node["next"] == None:
            temp["next"] = None 
            my_list["last"] = temp
        else:
            temp["next"] = node["next"]
            node["next"] = None
        my_list["size"] -= 1
    
    return my_list


def remove_first(my_list):
    """
    Elimina el primer elemento de la lista enlazada simple y devuelve su información.

    Parameters:
        my_list (single_linked_list): Lista enlazada simple.

    Returns:
        any: Información del nodo eliminado, o None si la lista está vacía.
    """
    if is_empty(my_list):
        return None
    
    else:
        to_delete = my_list["first"]
        my_list["first"] = to_delete["next"]
        if to_delete["next"] is None:
            my_list["last"] = None
        my_list["size"] -= 1
        return to_delete["info"]



def remove_last(my_list):
    """
    Elimina el último elemento de la lista enlazada simple y devuelve su información.

    Parameters:
        my_list (single_linked_list): Lista enlazada simple.

    Returns:
        any: Información del nodo eliminado, o None si la lista está vacía.
    """
    if is_empty(my_list):
        return None

    elif my_list["size"] == 1:
        to_delete = my_list["first"]
        my_list["first"] = None
        my_list["last"] = None
        my_list["size"] = 0
        return to_delete["info"]

    else:
        counter = 0
        node = my_list["first"]
        while counter < my_list["size"] - 1:
            temp = node
            node = node["next"]
            counter += 1
        temp["next"] = None
        my_list["last"] = temp
        my_list["size"] -= 1
        return node["info"]



def insert_element(my_list,element,pos):
    """
    Inserta un elemento en la posición dada.

    Parameters:
        my_list (single_linked_list): Lista enlazada simple.
        element: Elemento a insertar.
        pos (int): Posición en la cual insertar el elemento.

    Returns:
        single_linked_list: Lista actualizada.
    """
    if pos < 0 or pos >= size(my_list):
        return my_list
    else: 
        if type(element) != dict:
            new_node = {"info": element, "next": None}
        node = my_list["first"]
        
        if pos == 0:
            my_list["first"] = new_node
            new_node["next"] = node
            my_list["size"] += 1 
            return my_list
        counter = 0 
        while counter < pos: 
            temp = node
            node = node["next"]
            counter += 1 
        if temp["next"] == None:
            temp["next"] = new_node
            new_node["next"] = None 
            my_list["last"] = new_node
        else:
            temp["next"] = new_node
            new_node["next"] = node
        my_list["size"] += 1 
    return my_list

def change_info(my_list,pos,new_info):
    """
    Cambia la información de un nodo en la posición especificada.

    Parameters:
        my_list (single_linked_list): Lista enlazada.
        pos (int): Posición del nodo.
        new_info (any): Nueva información para reemplazar la actual.

    Returns:
        single_linked_list: Lista con la información cambiada.

    Raises:
        Exception: Si la posición no es válida.
    """
    if pos < 0 or pos >= size(my_list):
        raise Exception('IndexError: list index out of range')
    
    node = my_list["first"]
    if pos == 0: 
        node["info"] = new_info
        return my_list
    counter = 0 
    while counter < pos: 
        node = node["next"]
        counter += 1 
    node["info"] = new_info
    return my_list

def exchange(my_list, pos_1, pos_2):
    """
    Intercambia los valores de dos nodos en posiciones dadas.

    Parameters:
        my_list (single_linked_list): Lista enlazada.
        pos_1 (int): Posición del primer nodo.
        pos_2 (int): Posición del segundo nodo.

    Returns:
        single_linked_list: Lista con los nodos intercambiados.

    Raises:
        Exception: Si alguna posición no es válida.
    """
    if pos_1 < 0 or pos_1 >= size(my_list) or pos_2 < 0 or pos_2 >= size(my_list):
        raise Exception('IndexError: list index out of range')
    
    if pos_1 == pos_2:
        return my_list
    
    node1 = my_list["first"]
    node2 = my_list["first"]
    counter1 = 0
    counter2 = 0

    while counter1 < pos_1:
        node1 = node1["next"]
        counter1 += 1
    
    while counter2 < pos_2:
        node2 = node2["next"]
        counter2 += 1
    
    temp = node1["info"]
    node1["info"] = node2["info"]
    node2["info"] = temp
    return my_list

def sub_list(my_list, pos, num_elements):
    """
    Retorna una sublista de la lista original a partir de una posición y un número de elementos.

    Parameters:
        my_list (single_linked_list): Lista enlazada.
        pos (int): Posición inicial de la sublista.
        num_elements (int): Número de elementos que tendrá la sublista.

    Returns:
        single_linked_list: Nueva lista con los elementos seleccionados.

    Raises:
        Exception: Si la posición o el rango no son válidos.
    """
    if pos < 0 or pos >= size(my_list):
        raise Exception('IndexError: list index out of range')
    if num_elements < 0 or pos + num_elements > size(my_list):
        raise Exception('IndexError: list index out of range')
    
    sublist = {"first": None, "last": None, "size": 0}
    node = my_list["first"]
    counter = 0
    
    while counter < pos:
        node = node["next"]
        counter += 1
    
    for n in range(num_elements):
        new_node = {"info": node["info"], "next": None}
        if sublist["first"] is None:
            sublist["first"] = new_node
            sublist["last"] = new_node
        else:
            sublist["last"]["next"] = new_node
            sublist["last"] = new_node
        sublist["size"] += 1
        node = node["next"]
    
    return sublist

def default_sort_criteria(element_1, element_2):
   is_sorted = False
   if element_1 < element_2:
      is_sorted = True
   return is_sorted

def selection_sort(my_list, default_sort_criteria):
#    my_list["elements"]
    current = my_list["first"]
    sz = size(my_list)
    i = 0
    last_start = current
    
    while i<=sz-1:
        i+=1
        start = current
        minimo = start
        prox = start["next"]
        while prox != None:
            if prox["info"]<minimo["info"]:
                minimo = prox
            prox = prox["next"]
            
        if minimo != start:
            hldr = start["info"]
            start["info"] = minimo["info"]
            minimo["info"] = hldr
        
        current = current["next"]
        i+=1
        
    return my_list

def insertion_sort(my_list, sort_criteria=default_sort_criteria):
    if my_list["first"] is None or my_list["first"]["next"] is None:
        return my_list

    sorted_head = None
    current = my_list["first"]

    while current is not None:
        next_node = current["next"]

        if sorted_head is None or sort_criteria(current["info"], sorted_head["info"]) == True:
            current["next"] = sorted_head
            sorted_head = current
        else:
            temp = sorted_head
            while temp["next"] is not None and sort_criteria(temp["next"]["info"], current["info"]) == True:
                temp = temp["next"]
            current["next"] = temp["next"]
            temp["next"] = current

        current = next_node

    my_list["first"] = sorted_head


    temp = my_list["first"]
    while temp and temp["next"]:
        temp = temp["next"]
    my_list["last"] = temp

    return my_list

def shell_sort(my_list, sort_criteria=default_sort_criteria):
    arr = []
    node = my_list["first"]
    while node is not None:
        arr.append(node["info"])
        node = node["next"]

    n = len(arr)
    h = 1
    while h < n // 3:
        h = 3 * h + 1

    while h >= 1:
        for i in range(h, n):
            temp = arr[i]
            j = i
            while j >= h and sort_criteria(temp, arr[j - h]) == True:
                arr[j] = arr[j - h]
                j -= h
            arr[j] = temp
        h //= 3

    newlist = new_list()
    for val in arr:
        add_last(newlist, val)
    return newlist

def merge_sort(my_list, default_sort_criteria):
    n = size(my_list)
    
    if n <= 1:
        return my_list
    #Detiene la recursión si la lista ya es muy pequeña
    
    mid = n//2
    left = sub_list(my_list,0,mid)
    right = sub_list(my_list,mid,n-mid) # Por si es impar el numero de elementos en el array
    
    left2 = merge_sort(left, default_sort_criteria)
    right2 = merge_sort(right, default_sort_criteria)
    
    return join(left2,right2)

def join(left,right):

    res = new_list()
    i = left["first"]
    j = right["first"]
    
    while i != None and j != None:
        if default_sort_criteria(i["info"], j["info"]):
            add_last(res,i["info"])
            i = i["next"]
        else:
            add_last(res,j["info"])
            j = j["next"]
    
     # Por si es impar el numero de elementos en el array o i/j supera la longitud de su parte respectiva antes que la otra
    while i != None:
        add_last(res, i)
        i = i["next"]
    
    while j != None:
        add_last(res,j)
        j = j["next"]
    
    res["size"] = size(res)
    #Funcionan como extend en array
    
    return res

def quick_sort(my_list, default_sort_criteria):

    if size(my_list) <= 1:
        return my_list
    
    pivot = my_list["first"]["info"]

    left = new_list()
    right = new_list()
    equal = new_list()

    current = my_list["first"]
    while current is not None:
        if current["info"] == pivot:
            add_last(equal, current["info"])
        elif default_sort_criteria(current["info"], pivot):
            add_last(left, current["info"])
        else:
            add_last(right, current["info"])
        current = current["next"]

    left_sorted = quick_sort(left, default_sort_criteria)
    right_sorted = quick_sort(right, default_sort_criteria)

    res = new_list()
    i = left_sorted["first"]
    while i is not None:
        add_last(res, i["info"])
        i = i["next"]

    j = equal["first"]
    while j is not None:
        add_last(res, j["info"])
        j = j["next"]

    k = right_sorted["first"]
    while k is not None:
        add_last(res, k["info"])
        k = k["next"]

    res["size"] = size(left_sorted) + size(equal) + size(right_sorted)
    return res