setwd("~/Desktop/working/Thesis/liwc")

library(brms)
options(mc.cores = parallel::detectCores())
df <- read.csv("./data/consolidated.csv")

hyp3 = c('posemo','negemo')

preds = paste0(hyp3,collapse = ' * ')
string <- paste0("censored ~ ",preds)
form <- formula(string)

brm1 <- brm(form,df,family = zero_inflated_negbinomial())