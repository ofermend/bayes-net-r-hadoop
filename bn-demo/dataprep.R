# Adapted from http://scg.sdsu.edu/dataset-adult_r/

dataprep <- function(inp) { 
  out = c()
  
  data = switch(as.character(inp['marital']),
	'Never-married' = 'Never-Married',
	'Married-AF-spouse' = 'Married',
	'Married-civ-spouse' = 'Married',
	'Married-spouse-absent' = 'Not-Married',
	'Separated' = 'Not-Married',
	'Divorced' = 'Not-Married',
	'Widowed' = 'Widowed',
	'Other')
  out['marital'] = data
 
  data = as.character(inp['country'])
  if (grepl('?',data,fixed=T)) { data = 'Unknown' }
  else { 
    data = switch(as.character(inp['country']),
  	"Cambodia" = "SE-Asia",
  	"Canada" = "British-Commonwealth", 
  	"China" = "China", 
  	"Columbia" = "South-America", 
  	"Cuba" = "Other", 
  	"Dominican-Republic" = "Latin-America", 
  	"Ecuador" = "South-America", 
  	"El-Salvador" = "South-America", 
  	"England" = "British-Commonwealth", 
  	"France" = "Euro_1", 
  	"Germany" = "Euro_1", 
  	"Greece" = "Euro_2", 
  	"Guatemala" = "Latin-America", 
  	"Haiti" = "Latin-America", 
  	"Holand-Netherlands" = "Euro_1", 
  	"Honduras" = "Latin-America", 
  	"Hong" = "China", 
  	"Hungary" = "Euro_2", 
  	"India" = "British-Commonwealth",
  	"Iran" = "Other",
  	"Ireland" = "British-Commonwealth",
  	"Italy" = "Euro_1",
  	"Jamaica" = "Latin-America", 
  	"Japan" = "Other",
  	"Laos" = "SE-Asia",
  	"Mexico" = "Latin-America",
  	"Nicaragua" = "Latin-America",
  	"Outlying-US(Guam-USVI-etc)" = "Latin-America",
  	"Peru" = "South-America",
  	"Philippines" = "SE-Asia", 
  	"Poland" = "Euro_2",
  	"Portugal" = "Euro_2",
  	"Puerto-Rico" = "Latin-America",
  	"Scotland" = "British-Commonwealth",
  	"South" = "Euro_2",
  	"Taiwan" = "China", 
  	"Thailand" = "SE-Asia",
  	"Trinadad&Tobago" = "Latin-America", 
  	"United-States" = "United-States",
  	"Vietnam" = "SE-Asia",
  	"Yugoslavia" = "Euro_2",
	"Other") 
  }
  out['country'] = data
  
  data = as.character(inp['education']) 
  data = gsub("^10th","Dropout",data) 
  data = gsub("^11th","Dropout",data) 
  data = gsub("^12th","Dropout",data) 
  data = gsub("^1st-4th","Dropout",data) 
  data = gsub("^5th-6th","Dropout",data) 
  data = gsub("^7th-8th","Dropout",data) 
  data = gsub("^9th","Dropout",data) 
  data = gsub("^Assoc-acdm","Associates",data) 
  data = gsub("^Assoc-voc","Associates",data) 
  data = gsub("^Bachelors","Bachelors",data) 
  data = gsub("^Doctorate","Doctorate",data) 
  data = gsub("^HS-Grad","HS-Graduate",data) 
  data = gsub("^Masters","Masters",data) 
  data = gsub("^Preschool","Dropout",data) 
  data = gsub("^Prof-school","Prof-School",data) 
  data = gsub("^Some-college","HS-Graduate",data)
  out['education'] = data
  
  data = as.character(inp['type_employer'])
  if (grepl('?',data,fixed=T)) { data = 'Unknown' }
  else { 
    data = gsub("^Federal-gov","Federal-Govt",data) 
    data = gsub("^Local-gov","Other-Govt",data) 
    data = gsub("^State-gov","Other-Govt",data) 
    data = gsub("^Private","Private",data) 
    data = gsub("^Self-emp-inc","Self-Employed",data) 
    data = gsub("^Self-emp-not-inc","Self-Employed",data) 
    data = gsub("^Without-pay","Not-Working",data) 
    data = gsub("^Never-worked","Not-Working",data) 
  }
  out['type_employer'] = data

  data = as.character(inp['occupation'])
  if (grepl('?',data,fixed=T)) { data = 'Unknown' }
  else {
    data = gsub("^Adm-clerical","Admin",data) 
    data = gsub("^Armed-Forces","Military",data) 
    data = gsub("^Craft-repair","Blue-Collar",data) 
    data = gsub("^Exec-managerial","White-Collar",data) 
    data = gsub("^Farming-fishing","Blue-Collar",data) 
    data = gsub("^Handlers-cleaners","Blue-Collar",data) 
    data = gsub("^Machine-op-inspct","Blue-Collar",data) 
    data = gsub("^Other-service","Service",data) 
    data = gsub("^Priv-house-serv","Service",data) 
    data = gsub("^Prof-specialty","Professional",data) 
    data = gsub("^Protective-serv","Other-Occupations",data) 
    data = gsub("^Sales","Sales",data) 
    data = gsub("^Tech-support","Other-Occupations",data) 
    data = gsub("^Transport-moving","Blue-Collar",data) 
  }
  out['occupation'] = data

  data = switch(as.character(inp['race']),
  	"White" = "White", 
  	"Black" = "Black", 
  	"Amer-Indian-Eskimo" = "Amer-Indian", 
  	"Asian-Pac-Islander" = "Asian", 
  	"Other")
  out['race'] = data

  data = as.numeric(inp['capital_gain'])
  if (data <= 0) { out['capital_gain'] = 'None' }
  else if (data <= 4100) { out['capital_gain'] = 'Low' }
  else if (data <= 5000) { out['capital_gain'] = 'Med' }
  else { out['capital_gain'] = 'High' }
  
  data = as.numeric(inp['capital_loss'])
  if (data <= 0) { out['capital_loss'] = 'None' }
  else if (data <= 1500) { out['capital_loss'] = 'Low' }
  else if (data <= 2000) { out['capital_loss'] = 'Med' }
  else { out['capital_loss'] = 'High' }
  
  out['sex'] = inp['sex']
  out['relationship'] = inp['relationship']
  out['income'] = gsub("50K.","50K",as.character(inp['income']))
  out['age'] = as.character(cut(as.numeric(inp['age']), breaks=c(0, 18, 25, 34, 45, 50, 55, 60, 80, 120), right=F))
  out['hr_per_week'] =  as.character(cut(as.numeric(inp['hr_per_week']), breaks=c(0, 20, 40, 50, 60, 100), right=F))

  df = data.frame(t(rep(NA, length(out))))
  names(df) = names(out)
  df[1,] = out
  return(df)
} 

                       
