from DataStructures.List import array_list as lt
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf


def default_compare(key, entry):

   if key == me.get_key(entry):
      return 0
   elif key > me.get_key(entry):
      return 1
   return -1

def is_available(table, pos):

   entry = lt.get_element(table, pos)
   if me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
      return True
   return False

def find_slot(my_map, key, hash_value):
   first_avail = None
   found = False
   ocupied = False
   while not found:
      if is_available(my_map["table"], hash_value):
            if first_avail is None:
               first_avail = hash_value
            entry = lt.get_element(my_map["table"], hash_value)
            if me.get_key(entry) is None:
               found = True
      elif default_compare(key, lt.get_element(my_map["table"], hash_value)) == 0:
            first_avail = hash_value
            found = True
            ocupied = True
      hash_value = (hash_value + 1) % my_map["capacity"]
   return ocupied, first_avail

def rehash(my_map):
    old_table = my_map["table"]
    old_capacity = my_map["capacity"]
    new_capacity = mf.next_prime(2 * old_capacity)
    new_table = lt.new_list()
    i = 0
    while i < new_capacity:
        entry = me.new_map_entry(None, None)
        lt.add_last(new_table, entry)
        i = i + 1
    my_map["table"] = new_table
    my_map["capacity"] = new_capacity
    my_map["size"] = 0
    my_map["current_factor"] = 0
    i = 1
    while i < lt.size(old_table):
        entry = lt.get_element(old_table, i)
        if not is_available(old_table,i):
            key = me.get_key(entry)
            value = me.get_value(entry)
            put(my_map, key, value)
        i = i + 1
    return my_map

def new_map(num_elements, load_factor, prime=109345121):
    capacity = int(num_elements / load_factor)
    capacity = mf.next_prime(capacity)
    table = lt.new_list()
    i = 0
    while i < capacity:
        entry = me.new_map_entry(None, None)
        lt.add_last(table, entry)
        i = i + 1
    my_table = {}
    my_table["prime"] = prime
    my_table["capacity"] = capacity
    my_table["scale"] = 1
    my_table["shift"] = 0
    my_table["table"] = table
    my_table["current_factor"] = 0
    my_table["limit_factor"] = load_factor
    my_table["size"] = 0
    return my_table

def put(my_map, key, value):
    hv = mf.hash_value(my_map, key)
    result = find_slot(my_map, key, hv)
    found = result[0]
    pos = result[1]
    if found is True:
        entry = lt.get_element(my_map["table"], pos)
        me.set_value(entry, value)
    else:
        entry = lt.get_element(my_map["table"], pos)
        me.set_key(entry, key)
        me.set_value(entry, value)
        my_map["size"] = my_map["size"] + 1
        my_map["current_factor"] = my_map["size"] / my_map["capacity"]
        if my_map["current_factor"] > my_map["limit_factor"]:
            my_map = rehash(my_map)
    return my_map

def contains(my_map, key):
    hv = mf.hash_value(my_map, key)
    result = find_slot(my_map, key, hv)
    found = result[0]
    if found is True:
        return True
    return False

def get(my_map, key):
    hv = mf.hash_value(my_map, key)
    result = find_slot(my_map, key, hv)
    found = result[0]
    pos = result[1]
    if found is True:
        entry = lt.get_element(my_map["table"], pos)
        value = me.get_value(entry)
        return value
    return None

def remove(my_map, key):
    hv = mf.hash_value(my_map, key)
    result = find_slot(my_map, key, hv)
    found = result[0]
    pos = result[1]
    if found is True:
        entry = lt.get_element(my_map["table"], pos)
        me.set_key(entry, "__EMPTY__")
        me.set_value(entry, "__EMPTY__")
        my_map["size"] = my_map["size"] - 1
        my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    return my_map

def size(my_map):
    return my_map["size"]

def key_set(my_map):
    keys = lt.new_list()
    
    for elem in my_map["table"]:
        if elem != None:
            lt.add_last(keys,elem["key"])

    return keys

def value_set(my_map):
    keys = lt.new_list()
    
    for elem in my_map["table"]:
        if elem != None:
            lt.add_last(keys,elem["value"])

    return keys