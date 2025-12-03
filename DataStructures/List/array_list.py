def new_list():
    newlist = {
        "elements":[],
        "size":0
    }
    return newlist


def get_element(my_list, index):
    return my_list["elements"][index]


def is_present(my_list, element, cmp_function):
    
    size = my_list["size"]
    if size > 0:
        keyexist = False
        for keypos in range(0,size):
            info = my_list["elements"][keypos]
            if cmp_function(element,info) == 0:
                keyexist = True
                break
        if keyexist:
            return keypos
    return -1


def add_first(my_list, element):
    my_list["elements"].insert(0,element)
    my_list["size"] += 1
    return my_list


def add_last(my_list, element):
    
    my_list["elements"].append(element)
    my_list["size"] += 1
    return my_list

def size(my_list):
    # ls = my_list["size"]
    ls = len(my_list["elements"])
    return ls

def first_element(my_list):
    if size(my_list) == 0:
        f_element = None
    else:
        f_element = my_list["elements"][0]
    return f_element

def is_empty(my_list):
    res = False
    if len(my_list["elements"]) == 0: # or if my_list["size"] == 0
        res = True
    return res

def last_element(my_list):
    last_pos = len(my_list["elements"])-1
    res = my_list["elements"][last_pos]
    return res

def delete_element(my_list,pos):
    del my_list["elements"][pos]
    my_list["size"] -= 1
    return my_list

def remove_first(my_list):
    x = my_list["elements"].pop(0)
    my_list["size"] -= 1
    return x

def remove_last(my_list):
    last_pos = len(my_list["elements"])-1
    x = my_list["elements"].pop(last_pos)
    my_list["size"] -= 1
    return x

def insert_element(my_list, element, pos):
    my_list["elements"].insert(pos,element)
    my_list["size"] += 1
    return my_list

def change_info(my_list, pos, new_info):
    my_list["elements"][pos] = new_info
    return my_list

def exchange(my_list, pos_1, pos_2):
    hldr1= my_list["elements"][pos_1]
    hldr2= my_list["elements"][pos_2]
    
    my_list["elements"][pos_1] = hldr2
    my_list["elements"][pos_2] = hldr1
    
    return my_list

def sub_list(my_list, pos_i, num_elements):
    hldr = my_list["elements"][pos_i:(pos_i+num_elements)]
    res = {"elements":hldr,
        "size":num_elements
    }
    return res

def default_sort_criteria(element_1, element_2):

   is_sorted = False
   if element_1 < element_2:
      is_sorted = True
   return is_sorted

def selection_sort(my_list,sort_crit):
    list = my_list["elements"]
    for i in range(0,len(list)-1):
        minimo = min(list[i:len(list)-1])
        if minimo != list[i]:
            indice = list.index(minimo)
            temp = list[i]
            list[indice] = temp
            list[i] = minimo 
    return my_list
            

def insertion_sort(my_list, default_sort_criteria):
    for i in range(1,len(my_list["elements"])-1):
        last = my_list["elements"][i-1]
        current = my_list["elements"][i]
        j = i-1
        k = i
        while j>=0 or default_sort_criteria(last, current) == False:
            my_list["elements"][k] = last
            my_list["elements"][j] = current
            last = my_list["elements"][j]
            current = my_list["elements"][k]
            j -= 1
            k -= 1

    return my_list

def shell_sort(my_list, default_sort_criteria):
    #Fórmula de knuth: x = (3x+1)
    n = len(my_list["elements"])
    h = 1
    while h<n//3:
        h = (3*h)+1
    
        if my_list["elements"]==[] or n == 1:
            return my_list 
        
    while h>=1:
        i = 0
        j = i+h
        while j < n:
            first = my_list["elements"][i]
            last = my_list["elements"][j]
            if default_sort_criteria(first,last) == False:
                my_list["elements"][i] = last
                my_list["elements"][j] = first
            i += 1
            j += 1
        h = h//3
        
    
    return my_list

def merge_sort(my_list, default_sort_criteria):
    n = size(my_list)
    
    if n <=1:
        return my_list
    #Detiene la recursión si la lista ya es muy pequeña
    
    mid = n//2
    left = sub_list(my_list,0,mid)
    right = sub_list(my_list,mid,n-mid) # Por si es impar el numero de elementos en el array
    
    left2 = merge_sort(left, default_sort_criteria)
    right2 = merge_sort(right, default_sort_criteria)
    #Repite hasta que el if detecte que la lista es muy pequeña
    return join(left2,right2)

def join(left,right):
    
    res = new_list()
    i = 0
    j = 0
    while i<len(left["elements"]) and j<len(right["elements"]):
        if default_sort_criteria(left["elements"][i], right["elements"][j]):
            res["elements"].append(left["elements"][i])
            i+=1

        else:
            res["elements"].append(right["elements"][j])
            j +=1
    
    # Por si es impar el numero de elementos en el array o i/j supera la longitud de su parte respectiva antes que la otra
    res["elements"].extend(left["elements"][i:])
    res["elements"].extend(right["elements"][j:])
    res["size"] = size(res)
    
    return res

def quick_sort(my_list, default_sort_criteria):
    n = size(my_list)

    if n <= 1:
        return my_list

    pivot = my_list["elements"][0]
    left = new_list()
    right = new_list()
    equal = new_list()

    for elem in my_list["elements"]:
        if elem == pivot:
            left["elements"].append(elem)
            left["size"] += 1
        elif default_sort_criteria(elem, pivot):
            right["elements"].append(elem)
            right["size"] += 1
        else:
            equal["elements"].append(elem)
            equal["size"] += 1

    left_sorted = quick_sort(left, default_sort_criteria)
    right_sorted = quick_sort(right, default_sort_criteria)

    res = new_list()
    for e in left_sorted["elements"]:
        res["elements"].append(e)
        res["size"] += 1
    for e in equal["elements"]:
        res["elements"].append(e)
        res["size"] += 1
    for e in right_sorted["elements"]:
        res["elements"].append(e)
        res["size"] += 1

    return res