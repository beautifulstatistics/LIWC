setwd("~/Desktop/working/Thesis/liwc")

df <- read.csv("./data/consolidated.csv")

# breaks = 25
# 
# dfn <- sapply(df[names(df)[1:8]],FUN = cut, breaks=breaks, labels = 0:(breaks-1))
# dfn <- data.frame(dfn)
# dfn <- sapply(dfn,FUN = as.numeric)
# dfn <- data.frame(dfn)
# 
# dfn$censored <- df$censored
# dfn$not_censored <- df$not_censored
# 
# dfa <- aggregate(dfn,by = as.list(dfn[1:nrow(dfn),1:8]), FUN = sum)

hyps <- list(
  hyp0 = c('1'),
  hyp1 = c('ppron','drives','relativ','percept'),
  hyp2 = c('cogproc','totallen'),
  hyp3 = c('posemo','negemo')
)


counter <- 1
for (i in hyps){
  preds = paste0(i,collapse = ' * ')
  string <- paste0("censored ~ ",preds)
  form <- formula(string)
  
  model <- lm(form,df)
  
  print(summary(model))
  
  save(model, file = paste0("./models/",counter,".lm"))
  
  residual <- resid(model)
  actual <- fitted(model)

  png(paste0("./graphs/",counter,".png"))
  plot(actual,residual)
  dev.off()
  
  counter <- counter + 1
}