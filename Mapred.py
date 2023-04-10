import multiprocessing as mp

class MapReduce:
    def __init__(self, input):
        self.input = input
        self.output = []
        self.result = []

    # --- mapper function ---
    # Input  : line of chunk
    # Output : [(key, value)] - list of tuple
    def mapper(self, line):
        output_mapper = []
        item = line.strip().split(',')
        # When ID is located in the First column
        id_ = item[0]
        output_mapper.append((id_, 1))
        return output_mapper

    # --- shuffler function ---
    # Input  : kv pairs in output of mappers (list of tuples)
    # Output : {index of reducer: [(key, value), (key, value), ...]} - dictionary
    def shuffler(self, kv_pairs, num_reducers):
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
    # Input  : shuffled list of key-value pairs
    # Output : [(key: value)] - list of tuple
    def reducer(self, shuffled_data):
        # Set initial values for variables
        key = None
        current_key = ""
        current_value = 0
        reduced_list = []

        for kv_pair in shuffled_data:
            key, value = kv_pair
            # In this solution, value is fixed to 1 (integer), but in the general solution, value is likely to be a string, so the convert function is built in
            try:
                value = int(value)
            except ValueError:
                continue
            # If the key (ID) from the previous loop is the same as the current key, add value.
            if current_key == key:
                current_value += value
            # If the key (ID) from the previous loop is different from the current key, add the value calculated so far to the reduce result
            else:
                if current_key:
                    reduced_list.append((current_key, current_value))
                current_key = key
                current_value = value
        # If the value in the last loop is valid, add it to the final result
        if current_key == key:
            reduced_list.append((current_key, current_value))

        return (reduced_list)

    # --- find the passenger(s) having had the highest number of flights
    # Input  : reduced list of key-value pairs
    # Output : highest key-value pair(s)
    def find_highest_flights(self, reduced_data):
        #  Set initial values for variables
        highest_passenger_group = []
        highest_flights_count = 0

        for kv_pairs in reduced_data:
            for kv_pair in kv_pairs:
                _, value = kv_pair
                # Find the highest value in key-value pairs
                if value > highest_flights_count:
                    highest_passenger_group = [kv_pair]
                    highest_flights_count = value
                # If there are multiple passengers with the maximum value
                elif value == highest_flights_count:
                    highest_passenger_group.append(kv_pair)
        
        return highest_passenger_group
    
    def run(self):
        # Assign input of mapper as list of each line
        with open(self.input, encoding="utf8") as file:
            input_mapper = file.read().splitlines()

        # Assign available processors to a pool
        with mp.Pool(processes=mp.cpu_count()) as pool:
            # Use pool.map() to have each processor iteratively execute input_mapper to mapper()
            # Chunking to distribute input data to each processor as well
            output_mapper = pool.map(self.mapper, input_mapper, chunksize=int(len(input_mapper)/mp.cpu_count()))
            
            num_reducer = mp.cpu_count()

            shuffled_data = self.shuffler(output_mapper, num_reducer)
            
            # Unpack shuffled_data from dictionary to list for iteration
            input_reducer = [shuffled_data[i] for i in range(num_reducer)]

            # Each output from reducers comes into one list
            output_reducer = pool.map(self.reducer, input_reducer)
            
            self.output = output_reducer
            self.result = self.find_highest_flights(output_reducer)

if __name__ == '__main__':

    # input file
    mapreduce = MapReduce(".\AComp_Passenger_data_no_error.csv")
    
    mapreduce.run()
    print(mapreduce.result)
    
    # Save the results of reducers into a text file
    with open("output_reducer.txt", "w") as file:
        # Convert non-string items to string and list of strings to a single one
        file.write("\n".join(str(x) for x in mapreduce.output))
    
    # Save the final result of map reduce
    with open("highest_passenger.txt", "w") as file:
        # Convert non-string items to string and list of strings to a single one
        file.write("\n".join(str(x) for x in mapreduce.result))