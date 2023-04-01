# --- mapper function ---
# Input  : line of chunk
# Output : [key, value] - list
#          [passenger_id, 1] in this case
def mapper(line):
    output_mapper = []
    item = line.strip().split(',')
    # When ID is located in the First column
    id_ = item[0]
    output_mapper.append([id_, 1])
    return output_mapper

# --- shuffler function ---
# Input  : kv pairs in output of mappers
# Output : {key: [values]} - dictionary
def shuffler(kv):
    output_shuffler = {}
    for line in kv:
        key, value = line[0]
        if key not in output_shuffler:
            output_shuffler[key] = [value]
        else:
            output_shuffler[key].append(value)
    return output_shuffler

# --- reducer function ---
# Input  : 
# Output : {key: value} - dictionary
def reducer(kvs):
    output_reducer = {}
    key, values = kvs
    output_reducer[key] = sum(values)

    return output_reducer