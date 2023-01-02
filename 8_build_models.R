setwd("~/Desktop/working/Thesis/liwc")

df = read.csv("./data/consolidated_binned.csv")

hyps <- list(
  hyp0 = c('1'),
  hyp1 = c('ppron','drives','relativ','percept'),
  hyp2 = c('cogproc','totallen'),
  hyp3 = c('posemo','negemo')
)

counter <- 0
for (i in hyps){
  preds = paste0(i,collapse = ' * ')
  string <- paste0("cbind(censored,not_censored) ~ ",preds)
  form <- formula(string)
  
  print(form)
  model <- glm(form,df,family=quasibinomial())
  
  mods <- paste0("./models/",counter,".quasibinomial")
  print(mods)
  
  save(model, file = mods)
  
  counter <- counter + 1
}
