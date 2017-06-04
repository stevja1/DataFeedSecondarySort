# Data Feed Secondary Sort
An example of some code that performs simple operations on Adobe Analytics Click Stream Data Feed data. This project differs from the DataFeedHadoop project in that it implements a custom key type and custom comparitor that enable an efficient secondary sort. As a result, this Hadoop job should be very memory efficient and should scale very well -- especially as the number of hits per visitor increases.

To compile, run `mvn clean package`

Generate some data to test with by using the Data Feed Generator program.
```
$ java -cp target/DataFeedHadoop-1.0-SNAPSHOT.jar com.datafeedtoolbox.examples.DataFeedGenerator
```
When it's complete, you'll see something like this:
```
Average time: 3910ms
```
To run this code on a Hadoop cluster, you'll need to do the following:
1. Copy an example data feed file to HDFS -- by default, the DataFeedGenerator program creates a file called 1MM_datafeed_data.txt
2. Copy the sample columns file to HDFS -- in this repo, there is a file called 'sample_column_headers.tsv' that can be used.
3. Copy the jar file to the resource mangager on the cluster.
4. Run the hadoop program using the following command:
```
hadoop jar DataFeedHadoop-1.0-SNAPSHOT.jar com.datafeedtoolbox.examples.ScalableDataFeedJob /config/file/location.txt /example/data/feed/data/ /output/directory
```
Where the following applies:
* /config/file/location.txt is the location of the column headers in HDFS
* /example/data/feed/data/ is the location of the data feed data in HDFS
* /output/directory is the location where Hadoop will put the output (a list of part-r-* files)

Using the above command, Hadoop will only spin up one reducer. To use more than one reducer, add the `-Dmapreduce.job.reduces=XX` flag to the command, where XX is the number of reduce processes to run.

The output will be a list of visitor IDs followed by a revenue count. For example:
```
00006784233691479073:12863285679651659438  0.0
00009498476791980377:03236587504157723556  523.92
00010049514684546843:13212908716588764960  654.9
00011771127104880632:08673705166786179689  1178.82
00032971815294672161:03518159239083752812  392.93999999999994
00042724951824875744:10169201599906561232  0.0
00043454625338152274:08009488008828148434  261.96
00044633319700084602:02965038357916347251  130.98
00046809857184839428:09099550986264408756  130.98
00053555631620665722:01892731438398316400  523.92
```

Please reach out on Twitter (@BikerJared) if you have any questions!
