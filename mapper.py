public static class UPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text key = new Text();
    private IntWritable value = new IntWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String items[] = line.split("\t");
        String country = items[2];
        String population = items[4];
        if(country.matches("^[\\w\\-]+$") && population.matches("^\\d{1,9}$")) {
            key.set(country);
            value.set(Integer.parseInt(population));
            context.write(key, value);
        }
    }
}
