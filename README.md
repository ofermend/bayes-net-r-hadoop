<h1>Bayesian networks with R and Hadoop</h1>

Welcome to my demo of Bayesian Networks with R and Hadoop.

This page is accompanying my 2014 Hadoop Summit <a href="http://www.slideshare.net/slideshow/embed_code/35533375">talk</a>, about the same topic. In this page, I provide more details about the implementation, so that you can do-it-yourself. For the demo, I use the famous adult dataset. Please note that this dataset is not really large in the big-data sense, but I used it to exemplify the technique used, in a way that is easy to replicate the exercise on a single-node VM sandbox such as <a href=http://hortonworks.com/products/hortonworks-sandbox/>HDP Sandbox</a>


<h2>Installation</h2>

* Install <a href=http://hortonworks.com/products/hortonworks-sandbox/#install>HDP sandbox</a> into VMWare or Virtualbox. For this demo, I've used HDP 2.1 Sandbox.
* Make sure the HDP Sandbox on the VM has enough memory. At least 4GB is recommended, 8GB is better if possible.
* Login to the sandbox with username=root, password=hadoop
* as root, install R
```
yum update
yum install R
```
* as root, install needed R packages
```
export JAVA_HOME=/usr/jdk64/jdk1.7.0_45/
R CMD javareconf
R
> install.packages(c("bnlearn", "data.table"))
> install.packages(c("rJava", "Rcpp", "RJSONIO", "bitops", "digest", "functional", "stringr", "plyr", "reshape2", "caTools"))
```
* as root, install the RMR2 package
```
git clone https://github.com/RevolutionAnalytics/rmr2
cd rmr2
git checkout 3.1.0
R CMD build pkg/
R CMD INSTALL rmr2_3.1.0.tar.gz
```
* Switch to guest user: 
```
su - guest
```

* Clone this repository and move to demo folder: 
```
git clone https://github.com/ofermend/bayes-net-r-hadoop/
cd bayes-net-r-hadoop/bn-demo
```

Note that the demo folder (bayes-net-r-hadoop/bn-demo) contains 6 files:
* adult.dat: the training dataset
* adult.test: validation dataset
* adult.names: column descriptions
* dataprep.R: R function to transform raw data into features
* bn-demo.R: main R script to perform training and inference with RMR/Hadoop
* run-demo.sh: shell script to execute the whole demo

<h2>Quick review of the code</h2>
First we have to pre-process the data. For this demo, I adapted the general pre-processing flow described in <a href=http://scg.sdsu.edu/dataset-adult_r/>this blog post</a>. The final pre-processing step is implemented in the function dataprep() in dataprep.R.

Now let's review bn-demo.R

<h3>Initialization</h3>
* Starting off, we load the needed packages: bnlearn, data.table and rmr2. We also source the dataprep.R file so that we can call dataprep()
* We define the trim() function to trim a string from leading or trailing whitespace
```
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
```
* Loading the training data from the adult.data file:
```
data = read.table("adult.data", sep=",",header=F, stringsAsFactors=F,
                   col.names=c("age", "type_employer", "fnlwgt", "education", "education_num","marital", "occupation", "relationship", "race","sex", "capital_gain", "capital_loss", "hr_per_week","country", "income"), fill=FALSE,strip.white=T)
```
* We then transform the raw dataset into the desired features, in the variable "train"
```
cvt_data = do.call(rbind.data.frame, lapply(as.list(1:dim(data)[1]), function(x) dataprep(data[x[1],])))
train = as.data.frame(lapply(cvt_data, factor))
```
Note that we call dataprep() in the "lapply" row by row, and then rbind the results together into a single data frame. 

<h3>Learning</h3>
* We seed the network with a whilelist, and also define a few restrictions as a blacklist, and then use rsmax2() from bnlearn to learn the structure.
```
wl = data.frame(
  from = c('country', 'capital_gain', 'capital_loss', 'occupation', 'hr_per_week', 'education', 'sex', 'race', 'race'),
  to = c('race', 'income', 'income', 'income', 'income', 'occupation', 'hr_per_week', 'occupation', 'education')
)
bl = data.frame(
  from = c('marital', 'sex', 'relationship', 'marital'),
  to = c('race', 'race', 'sex', 'sex')
)
net = rsmax2(train, whitelist=wl, blacklist=bl, restrict='si.hiton.pc', maximize='tabu')
```
* Next, we learn the network probabilities - the CPT
```
fitted = bn.fit(net, train, method="bayes", iss=5)
```
We use the "bayes" method instead of the default, with the iss parameter set to 5.

<h3>Inference</h3>
Now that we have the network structure and parameters, we move to perform inference with RMR and Hadoop.
* Recall that our design is to use the mapper as a no-op (pass-through) it's more difficult to control the number of mappers. We use the reducers as the compute processes, and as we'll see later we can control the number of reducers quite easily with a parameter. Therefore our mapper is rather simple, using a random key to just pass the instances to any reducer for processing:
```
map_func <- function(., vals)
{
        key_list = list(); val_list = list()
        for (v in vals) {
                fvec = unlist(strsplit(v, ',', fixed=T))
                if (length(fvec)<15) { next; }
                names(fvec)=c("age", "type_employer", "fnlwgt", "education", "education_num","marital", "occupation", "relationship", "race","sex", "capital_gain", "capital_loss", "hr_per_week","country", "income")
                key = floor(runif(1,0,MAX_REDUCERS))
                key_list = c(key_list, key)
                val_list = c(val_list, v)
        }
        return (keyval(key_list, val_list))
}
```
* Our "reduce" function is where the real bayesian network inference occurs:
```
reduce_func <- function(., vals)
{
        key_list = list(); val_list = list()
        for (v in vals) {
                increment.counter('bn-demo', 'row', 1)
                fvec = sapply(strsplit(v, ',', fixed=T), trim)
                names(fvec)=c("age", "type_employer", "fnlwgt", "education", "education_num","marital", "occupation", "relationship", "race","sex", "capital_gain", "capital_loss", "hr_per_week","country", "income")
                pv = dataprep(fvec)
                evidence = as.list(pv[1,setdiff(colnames(pv), 'income')])
                write(paste(names(evidence), collapse="|", sep=""), stderr())
                write(paste(evidence, collapse="|", sep=""), stderr())
                prob = cpquery(fitted, event = (income == ">50K"), evidence = evidence, method="lw")
                key_list = c(key_list, v)
                val_list = c(val_list, format(prob, digits=2))
        }
        return (keyval(key_list, val_list))
}
```
* Now let's look at the invocation of RMR:
```
opt = rmr.options(backend = "hadoop",
                  backend.parameters = list(hadoop=list(D="mapreduce.reduce.memory.mb=1024",
                                                        D=paste0("mapreduce.job.reduces=", NUM_REDUCERS))))

inpFile = 'adult.test'
outFile = 'adult.out'
mapreduce(input=inpFile, input.format="text",
          output=outFile, output.format=make.output.format("csv", sep=","),
          map=map_func, reduce=reduce_func)
```

In rmr.options(), we increase the memory for the reducer to 1GB. This of course may need to be adjusted depending on the size of your bayesian network. We also set the number of reducers to NUM_REDUCERS. For this demo on a VM we set it to 3, but of course in a real world situation and a large clsuter can be 50, 100 or more. The more reducers, the more parallelism you would get.

Then we simply call rmr's mapreduce() function, giving it the input file, output file, mapper and reducer functions, and off we go. 

<h3>running the demo: run-demo.sh</h3>
I created a small script to execute the demo from start to finish. This script has a single parameter ($1) - a numeric with the number of rows for inference. For example, you can run this as follows:
```
./run-demo.sh 100
```
Which will infer for the first 100 rows (instances) in the adult.test file.

Let's look at this shell script:
```
hadoop fs -rm -r adult.test
hadoop fs -rm -r adult.out
if [ $1 -eq -1 ];
then
  hadoop fs -put adult.test adult.test
else
  head -$1 adult.test > temp1
  hadoop fs -put temp1 adult.test
  rm temp1
fi
export HADOOP_CMD=/usr/bin/hadoop
export HADOOP_STREAMING=/usr/lib/hadoop-mapreduce/hadoop-streaming-*.jar
Rscript bn-demo.R
hadoop fs -getmerge adult.out adult.out
```
* First, we remove any old files from HDFS, and copy the first $1 rows from adult.test into HDFS as "adult.test"
* Then, we run the bn-demo.R script. Note that it will kick off the necessary map-reduce job with RMR. We need 2 export statements as you see above to let RMR know which Hadoop and streaming to use.
* Finally, we copy back the output file from inference into the local folder so we can review it.

Let's take a look at the output:
```
Loading required package: bnlearn
Loading required package: data.table
Loading required package: rmr2
Loading required package: Rcpp
Loading required package: RJSONIO
Loading required package: bitops
Loading required package: digest
Loading required package: functional
Loading required package: reshape2
Loading required package: stringr
Loading required package: plyr
Loading required package: caTools
[1] "Learning structure"
14/05/23 13:30:28 WARN streaming.StreamJob: -file option is deprecated, please use generic option -files instead.
packageJobJar: [/tmp/RtmpuxcDAa/rmr-local-env46c4298421ad, /tmp/RtmpuxcDAa/rmr-global-env46c467216c19, /tmp/RtmpuxcDAa/rmr-streaming-map46c45461b065, /tmp/RtmpuxcDAa/rmr-streaming-reduce46c47bfd5d57] [/usr/lib/hadoop-mapreduce/hadoop-streaming-2.4.0.2.1.1.0-385.jar] /tmp/streamjob5500124204348219433.jar tmpDir=null
14/05/23 13:30:29 INFO client.RMProxy: Connecting to ResourceManager at sandbox.hortonworks.com/172.16.102.144:8050
14/05/23 13:30:30 INFO client.RMProxy: Connecting to ResourceManager at sandbox.hortonworks.com/172.16.102.144:8050
14/05/23 13:30:30 INFO mapred.FileInputFormat: Total input paths to process : 1
14/05/23 13:30:30 INFO mapreduce.JobSubmitter: number of splits:2
14/05/23 13:30:30 INFO Configuration.deprecation: mapred.textoutputformat.separator is deprecated. Instead, use mapreduce.output.textoutputformat.separator
14/05/23 13:30:31 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1400605562649_0005
14/05/23 13:30:31 INFO impl.YarnClientImpl: Submitted application application_1400605562649_0005
14/05/23 13:30:31 INFO mapreduce.Job: The url to track the job: http://sandbox.hortonworks.com:8088/proxy/application_1400605562649_0005/
14/05/23 13:30:31 INFO mapreduce.Job: Running job: job_1400605562649_0005
14/05/23 13:30:38 INFO mapreduce.Job: Job job_1400605562649_0005 running in uber mode : false
14/05/23 13:30:38 INFO mapreduce.Job:  map 0% reduce 0%
14/05/23 13:30:50 INFO mapreduce.Job:  map 67% reduce 0%
14/05/23 13:30:51 INFO mapreduce.Job:  map 100% reduce 0%
14/05/23 13:30:58 INFO mapreduce.Job:  map 100% reduce 25%
```
Here is an example of 6 rows from adult.out:
```
43, Private, 346189, Masters, 14, Married-civ-spouse, Exec-managerial, Husband, White, Male, 0, 0, 50, United-States, >50K.,0.75
40, Private, 85019, Doctorate, 16, Married-civ-spouse, Prof-specialty, Husband, Asian-Pac-Islander, Male, 0, 0, 45, ?, >50K.,0.64
34, Private, 238588, Some-college, 10, Never-married, Other-service, Own-child, Black, Female, 0, 0, 35, United-States, <=50K.,0.0019
23, Private, 134446, HS-grad, 9, Separated, Machine-op-inspct, Unmarried, Black, Male, 0, 0, 54, United-States, <=50K.,0.085
54, Private, 99516, HS-grad, 9, Married-civ-spouse, Craft-repair, Husband, White, Male, 0, 0, 35, United-States, <=50K.,0.099
46, State-gov, 106444, Some-college, 10, Married-civ-spouse, Exec-managerial, Husband, Black, Male, 7688, 0, 38, United-States, >50K.,0.93
```
Notice that for each row, the one-before-last column is the original income level (two values: "<=50K" or ">50K") - this is what we're trying to predict. The last column (which was added by our script output) is the (predicted) likelihood that this person's income is higher than 50K. You can find the full solution file at bn-demo/adult-solution.out.

<h3>Summary</h3>
I hope you enjoyed this walk-through of using R, RMR and Hadoop to infer with Bayesian Networks.
I'd love to hear of your own experiences applying these techniques to other problems in your domain.



