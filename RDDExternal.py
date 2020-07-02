# Print the file_path
print("The file_path is", file_path)

# Create a fileRDD from file_path
fileRDD = sc.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))

#Partitions in your data

# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions = 5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())


#Applying Transformations & Actions

# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x**3)

# Collect the results
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
for numb in numbers_all:
	print(numb)

---------------------------------
# Filter the fileRDD to select lines with Spark keyword
fileRDD_filter = fileRDD.filter(lambda line: 'Spark' in line)

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

# Print the first four lines of fileRDD
for line in fileRDD_filter.take(4):
  print(line)

#PairRDDS & Applying Transformation
# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize([(1,2),(3,4),(3,6),(4,5)])

# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x+y)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))

# Sort the reduced RDD with the key by descending order
Rdd_Reduced_Sort = Rdd_Reduced.sortByKey(ascending=False)

# Iterate over the result and print the output
for num in Rdd_Reduced_Sort.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))

# Transform the rdd with countByKey()
total = Rdd.countByKey()

# What is the type of total?
print("The type of total is", type(total))

# Iterate over the total and print the output
for k, v in total.items():
  print("key", k, "has", v, "counts")


  #Load #transform
  # Create a baseRDD from the file path
  baseRDD = sc.textFile(file_path)

  # Split the lines of baseRDD into words
  splitRDD = baseRDD.flatMap(lambda x: x.split())

  # Count the total number of words
  print("Total number of words in splitRDD:", splitRDD.count())

  #More Transformation, Removing the words
  # Convert the words in lower case and remove stop words from stop_words
  splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

  # Create a tuple of the word and 1
  splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

  # Count of the number of occurences of each word
  resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

  #More Transformation & Action

  # Display the first 10 words and their frequencies
  for word in resultRDD.take(10):
      print(word)

  # Swap the keys and values
  resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

  # Sort the keys in descending order
  resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

  # Show the top 10 most frequent words and their frequencies
  for word in resultRDD_swap_sort.take(10):
      print("{} has {} counts".format(word[1], word[0]))


#pairRdd
# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize([(1,2),(3,4),(3,6),(4,5)])

# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x+y)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))