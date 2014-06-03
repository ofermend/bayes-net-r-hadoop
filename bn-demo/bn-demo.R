require(bnlearn)
require(data.table)
require(rmr2)
source('dataprep.R')

NUM_REDUCERS = 4

# Function to trim leading + trailiing whitespace from string
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

# Load input training set
data = read.table("adult.data", sep=",",header=F, stringsAsFactors=F,
                   col.names=c("age", "type_employer", "fnlwgt", "education", "education_num","marital", "occupation", "relationship", "race","sex", "capital_gain", "capital_loss", "hr_per_week","country", "income"), fill=FALSE,strip.white=T) 

# Convert raw fields into features, and represent as factors
cvt_data = do.call(rbind.data.frame, lapply(as.list(1:dim(data)[1]), function(x) dataprep(data[x[1],])))
train = as.data.frame(lapply(cvt_data, factor))

# Whitelist
wl = data.frame(
  from = c('income', 'occupation', 'hr_per_week', 'education', 'country', 'type_employer'),
  to = c('capital_gain', 'income', 'income', 'occupation', 'education', 'income')
)
node_names = names(train)
bl = data.frame(
  from = rep(node_names, 3),
  to = c(rep('race', length(node_names)), rep('age', length(node_names)), rep('sex', length(node_names)))
)

# Learn network structure
print("Learning structure")
net = tabu(train, whitelist=wl, blacklist=bl, score='bde')

# Learn parameters
fitted = bn.fit(net, train, method="bayes", iss=5)

#
# Inference:
#   event to predict: income 
#   evidence: all other non-NA variables 
#

# Reduce: perform infernece one row at a time
reduce_func <- function(., values) 
{
	out_klist = list() 
	out_vlist = list()
	for (v in values) {

		# Increment counter so that Hadoop does not think reducer is "Dead"
		increment.counter('bn-demo', 'row', 1)

		fvec = sapply(strsplit(v, ',', fixed=T), trim)
                names(fvec)=c("age", "type_employer", "fnlwgt", "education", "education_num","marital", "occupation", "relationship", "race","sex", "capital_gain", "capital_loss", "hr_per_week","country", "income")
		pv = dataprep(fvec)

		# Generate evidence vector, and perform CPQuery()
		evidence = as.list(pv[1,setdiff(colnames(pv), 'income')])
		prob = cpquery(fitted, event = (income == ">50K"), evidence = evidence, method="lw")

		# Update output key/value lists
		out_klist = c(out_klist, v)
		out_vlist = c(out_vlist, format(prob, digits=2))
	}
	return (keyval(out_klist, out_vlist))
}

# map: do-nothing, just transition to reducer with "dummy" key of age+country string
map_func <- function(., values)
{
	out_klist = list() 
	out_vlist = list()
	for (v in values) {

		# Split row into fields
		fvec = unlist(strsplit(v, ',', fixed=T))
		
		# Ignore if row does not have all fields
		if (length(fvec)<15) { next; }
        
        # Create new random key
		key = floor(runif(1,0,NUM_REDUCERS))
		out_klist = c(out_klist, key)
		out_vlist = c(out_vlist, v)
	}
	return (keyval(out_klist, out_vlist))
}

#
# Inference with RMR
#
opt = rmr.options(backend = "hadoop",
		  backend.parameters = list(hadoop=list(D="mapreduce.reduce.memory.mb=1024",
						        D=paste0("mapreduce.job.reduces=", NUM_REDUCERS))))
inpFile = 'adult.test' 
outFile = 'adult.out'
mapreduce(input=inpFile, input.format="text",
	  output=outFile, output.format=make.output.format("csv", sep=","),
	  map=map_func, reduce=reduce_func)
