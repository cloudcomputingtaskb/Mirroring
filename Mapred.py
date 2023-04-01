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
        
        input_reducer = shuffler(output_mapper)
        
        output_reducer = pool.map(reducer, input_reducer.items(), chunksize=int(len(input_reducer.keys())/mp.cpu_count()))

        print(output_reducer)
        
if __name__ == '__main__':
    main()