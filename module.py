# --- mapper function ---
# Input  : line of chunk
# Output : [key, value] - list
#          [[passenger_id, 1], [], ...] in this case
def mapper(line):
    output_mapper = []
    item = line.strip().split(',')
    # When ID is located in the First column
    id_ = item[0]
    output_mapper.append([id_, 1])
    return output_mapper

# --- shuffler function ---
# Input  : kv pairs in output of mappers (list of lists)
# Output : {key: [values]} - dictionary
def shuffler(kv_pairs, num_reducers):
    # Create a dictionary of lists
    # Each list stores key and corresponding values
    shuffled_dict = {}

    # Assign index to each reducer
    for i in range(num_reducers):
        shuffled_dict[i] = []

    # Use hash for distributing data to the reducers
    for kv_pair in kv_pairs:
        key, value = kv_pair[0]
        reducer_idx = hash(key) % num_reducers
        shuffled_dict[reducer_idx].append((key, value))
    
    # Sort the distributed data in each reducer
    for i in range(num_reducers):
        shuffled_dict[i] = sorted(shuffled_dict[i], key=lambda x: x[0])

    return shuffled_dict

# --- reducer function ---
# Input  : 
# Output : {key: value} - dictionary
def reducer(shuffled_data):
    key = None

    current_key = ""
    current_value = 0

    reduced_list = []

    for kv_pair in shuffled_data:
        key, value = kv_pair
    
        if current_key == key:
            current_value += value
        else:
            if current_key:
                reduced_list.append([current_key, current_value])
            current_key = key
            current_value = value

    if current_key == key:
        reduced_list.append([current_key, current_value])

    return (reduced_list)