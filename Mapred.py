import multiprocessing as mp
from module import mapper, reducer, shuffler

def main():
    # input file path
    filepath = ".\AComp_Passenger_data_no_error.csv"

    # Assigning input of mapper as list of each line
    with open(filepath, encoding="utf8") as file:
        input_mapper = file.read().splitlines()

    # Assigning available processors to a pool
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Use pool.map() to have each processor iteratively execute input_mapper to mapper()
        # Chunking to distribute input data to each processor as well
        output_mapper = pool.map(mapper, input_mapper, chunksize=int(len(input_mapper)/mp.cpu_count()))
        
        num_reducer = mp.cpu_count()

        shuffled_data = shuffler(output_mapper, num_reducer)
        
        # Unpack shuffled_data from dictionary to list for iteration
        input_reducer = [shuffled_data[i] for i in range(num_reducer)]
        output_reducer = pool.map(reducer, input_reducer)

        print(output_reducer)
        
if __name__ == '__main__':
    main()