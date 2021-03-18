
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
