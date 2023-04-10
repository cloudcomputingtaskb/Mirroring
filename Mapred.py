import multiprocessing as mp
from module import mapper, reducer, shuffler, find_highest_flights

def main():
    # input file path
    filepath = ".\AComp_Passenger_data_no_error.csv"

    # Assign input of mapper as list of each line
    with open(filepath, encoding="utf8") as file:
        input_mapper = file.read().splitlines()

    # Assign available processors to a pool
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Use pool.map() to have each processor iteratively execute input_mapper to mapper()
        # Chunking to distribute input data to each processor as well
        output_mapper = pool.map(mapper, input_mapper, chunksize=int(len(input_mapper)/mp.cpu_count()))
        
        num_reducer = mp.cpu_count()

        shuffled_data = shuffler(output_mapper, num_reducer)
        
        # Unpack shuffled_data from dictionary to list for iteration
        input_reducer = [shuffled_data[i] for i in range(num_reducer)]

        # Each output from reducers comes into one list
        output_reducer = pool.map(reducer, input_reducer)

        result = find_highest_flights(output_reducer)
        print(result)
    
    # Save the results of reducers into a text file
    with open("output_reducer.txt", "w") as file:
        # Convert non-string items to string and list of strings to a single one
        file.write("\n".join(str(x) for x in output_reducer))
    
    # Save the final result of map reduce
    with open("highest_passenger.txt", "w") as file:
        # Convert non-string items to string and list of strings to a single one
        file.write("\n".join(str(x) for x in result))
        
if __name__ == '__main__':
    main()