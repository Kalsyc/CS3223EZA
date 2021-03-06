
# CS3223: Database Systems Implementation Group Project (AY20/21 S2)

## Group Members:
[Sim Jun Yuen, Darren](https://github.com/Kalsyc)<br>
[Lin Yuting](https://github.com/linyutinglyt)<br>
[Seah Lynn](https://github.com/seahlynn)<br>

This project is an implementation of a simple SPJ (Select-Project-Join) query engine to visualize how different query execution trees have different performance results, which will provide some motivation for query optimization.

## Available Operators & Types

Following the given template, our group has come up with these additional operators:

- Block Nested Loops join
- Sort Merge Join
- External Sort (k-way merge sort algorithm)
- `DISTINCT`
- `ORDERBY` (`DESC` only, assumes `ASC` if not specified)
- `GROUPBY`

Additionally, our query engine also supports the following types of data:
- String
- Integer
- Time (HH:MM:SS format)

## Instructions for Setup

1) This project can be run on Windows 10 or any Unix System (Tested only on Ubuntu 18.04 and MacOS). Ensure that you have Java SE11 & JDK installed in order to run the query engine.
2) Clone this repository from GitHub
3) In order to setup the environment, ensure that COMPONENT/classes is added to the classpath in your environment variables.
4) If on Windows, run the queryenv.bat script that can be downloaded [here](http://www.comp.nus.edu.sg/~tankl/cs3223/project/queryenv.bat). If you are using a Unix System, run `source queryenv` in order to setup the environment.
5) To build the project, run the `build.bat` (Windows) file or the `build.sh` (Unix) file and ensure that it compiles without any errors or warnings.

## How to experiment
### Changing the parser
1) If you wish to make changes to the parser, you can recompile the parser by CUP via the command `java java_cup.Main parser.cup` within the `COMPONENT/src/qp/parser` folder. 
2) You are also to modify the tokenizer in `scaner.lex` if you have added extra terminals/commands. 
3) Afterwards, you can compile the tokenizer via the command `java JLex.Main scaner.lex`.
4) Once the tokenizer has been recompiled, `scaner.lex.java` will be generated and you need to rename the file to `Scaner.java` and at the same time, replacing the class name from `Yylex` to `Scaner`.
5) Rebuild the project and ensure that it compiles without warnings or errors.

### Playing around with datasets
In our project, there are 2 folders (`testcases` and `experiment`) where you can play with the dataset that we have created. The `testcases` folder consists of the original testcases written in the template given to us for this project whilst the `experiment` folder consists of 4 different queries and 5 different tables that we have to work with for our project's demo.

In order to generate new data from the `.det` files, run `java RandomDB <tablename> <no. of records>` and then `java ConvertTxtToTbl <tablename>` right after.

Currently, the default tables in `experiment` have about 10,000 records each.

## Implementation Details/Modifications
### External Sort

The external sorting algorithm implemented in the query engine is the k-way merge sort algorithm learnt in lecture where sorted runs are generated and combined in some number of passes, depending on the number of available buffers. Sorted runs are generated by reading in batches of tuples from the base operator until all the buffers are full. The tuples are then sorted using an `AttributeSort` comparator which implements the `compareTuple` from the given `Tuple` class. The block is then temporarily written out to file.

After all the sorted runs are generated, we then merge the sorted runs until only one final sorted run remains. This final sorted run is then accessed by the operator whenever next() is called which pulls tuples from this final run and returns batches of sorted tuples when it is invoked.

### Block Nested Join

The Block Nested Join operator is implemented on top of the given Nested Join with (B - 2) buffer pages allocated to the left batch where B is the number of buffers. The change made to the Nested Join algorithm is that the left table is read in blocks of tupes instead of individual tuples which speeds up the algorithm.

### Sort Merge Join

The Sort Merge Join operator is implemented with the help of the External Sort which ensures that the left and right tables-to-be-joined are sorted first before we start the joining process. Afterwhich, we start the joining process by comparing the tuples on the left and right batches and see if they are able to be joined. The algorithm utilizes an `ArrayList` to keep track of the duplicate tuples that may be found on the right table so that we may be able to backtrack and join tuples if necessary.<br>

The algorithm initializes with a left pointer and a right pointer which point to the current considered tuples within the two batches. If the left tuple is larger than the right tuple, we increment the right pointer and traverse down the right tuple till we find a match or that the left tuple is smaller. If the left and right tuples finally match, we then add it to the array list and then join afterwards. However, if the left tuple is smaller than the right tuple, we traverse down the left until we either find a match or that the left tuple is larger than the right tuple.

### Distinct
The `DISTINCT` operator is implemented with the help of the `MergeSort` operator. Once a sorted input tuple is retrieved from the `MergeSort` operator, it is checked against the output buffer. If it already exists, the tuple is discarded, if not, it is added to the output buffer. 

### GroupBy
The `GroupBy` operator is implemented by sorting the tuples by the list of GroupBy attributes with the help of the `MergeSort` operator.

### OrderBy
The `OrderBy` operator is implemented by using the `MergeSort` operator, similar to the `GroupBy` operator. However, the parser had to be adapted in order to include the `DESC` keyword. If this `DESC` keyword is present in the query, the variables within the comparator in the `MergeSort` operator are then swapped to ensure that the tuples are sorted in reverse order. However, if the `DESC` keyword is missing, the query engine will assume that the tuples are to be sorted in `ASC` order.

In the parser, where `DESC` is implemented, isDesc boolean is set to true by the “s.setIsDesc” function. This boolean is passed to the OrderBy operator which passes it to the External Sort algorithm. In the external sort, depending on the value of isDesc, the AttributeComparator is switched to order the tuples in ascending / descending order. The OrderBy operator then pulls the resultant tuples from the sort. Hence the output tuples are successfully ordered accordingly. 
