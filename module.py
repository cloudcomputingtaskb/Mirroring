# Frame of each module
def mapper(x):
    return (x, 1)

def reducer(x, y):
    return x+y

def shuffler(output_mapper):
    output_shuffler = {}
    for key, value in output_mapper:
        if key not in output_shuffler:
            output_shuffler[key] = [value]
        else:
            output_shuffler[key].append(value)
    return output_shuffler

