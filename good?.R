setwd("~/Desktop/working/Thesis/liwc")

library(ggplot2)

df = read.csv("data/consolidatedhyp.csv",colClasses = 'numeric')
df = df[df$tokencount > 0,]

a = aggregate(df$censored, list(posemo = df$posemo, negemo=df$negemo, tokencount=df$tokencount),FUN=sum)
a$not_censored = aggregate(df$not_censored, list(posemo = df$posemo, negemo=df$negemo, tokencount=df$tokencount),FUN=sum)$x
names(a)[4] = 'censored'
a$N = a$censored + a$not_censored
a$observed= a$censored/a$N

a1 = a[a$censored > 0 & a$not_censored > 0,]
rownames(a1) = 1:dim(a1)[1]

a1.1 = a1[-c(1,5,2538,3108,4478,4477,2084, 4476),]

##

a3 = a1.1

if(TRUE){
  form <- formula(cbind(censored,not_censored) ~ posemo * negemo + tokencount)
  family = binomial()
  
  ma <- glm(form, a3, family = family)
  
  fit = fitted(ma)
  res = resid(ma)
  
} else {
  library(brms)
  options(mc.cores = 4)
  
  form <- bf(censored | trials(N) ~ posemo * negemo + tokencount)
  family <- binomial()
  
  make_stancode(form,data=a3, family= family)
  get_prior(form,data=a3, family = family)
  
  prior = prior(normal(0,1),class=b)
  
  mb <- brm(form, a3, family = family, 
           prior = prior,
           control = list(adapt_delta=.8),
           chains= 4)
  
  fit = data.frame(predict(mb, type='link'))$Estimate
  res = data.frame(resid(mb))$Estimate
  se = 0
}

logit <- function(p) {
  return(log(p / (1 - p)))
}

inverse_logit <- function(x) {
  return(exp(x) / (1 + exp(x)))
}

make_se <- function(name){
  tc = coef(ma)['(Intercept)'] + coef(ma)[name]*a3[name]
  tc = tc[[1]]
  setc = logit(2*inverse_logit(tc)*(1-inverse_logit(tc)))
  return(setc)
}
  
df2 = cbind(a3, fit, res, 
            sefit = logit(2*sqrt(fit*(1-fit))),
            setokencount = make_se('tokencount'),
            seposemo = make_se('posemo'),
            senegemo = make_se('negemo')
)

( 
  ggplot(df2)
  + geom_ribbon(aes(x=fit, ymin=-abs(sefit), ymax=abs(sefit), alpha = .2))
  + guides(alpha=FALSE)
  + geom_point(aes(fit,res))
  + geom_smooth(aes(fit,res),se=FALSE)
  + ggtitle('Residual vs Fitted')
  + ylab('Linear Residuals')
  + xlab('Fitted')
  + theme_minimal()
  + theme(plot.title = element_text(hjust = 0.5),
        plot.title.position = "plot")
)

( ggplot(df2)
  + geom_point(aes(tokencount,res))
  + geom_smooth(aes(tokencount,res),se=FALSE)
  + geom_ribbon(aes(x=tokencount, ymin=-abs(setokencount), ymax=abs(setokencount), alpha = .2))
  + guides(alpha=FALSE)
  + ggtitle('Residual vs tokencount')
  + ylab('Linear Residuals')
  + xlab('tokencount')
  + theme_minimal()
  + theme(plot.title = element_text(hjust = 0.5),
          plot.title.position = "plot")
)

( ggplot(df2) 
  + geom_point(aes(posemo,res))
  + geom_smooth(aes(posemo,res),se=FALSE)
  + geom_ribbon(aes(x=posemo, ymin=-abs(seposemo), ymax=abs(seposemo), alpha = .2))
  + guides(alpha=FALSE)
  + ggtitle('Residual vs posemo')
  + ylab('Linear Residuals')
  + xlab('posemo')
  + theme_minimal()
  + theme(plot.title = element_text(hjust = 0.5),
          plot.title.position = "plot")
  
)

( ggplot(df2)
  + geom_point(aes(negemo,res))
  + geom_smooth(aes(negemo,res),se=FALSE)
  + geom_ribbon(aes(x=negemo, ymin=-abs(senegemo), ymax=abs(senegemo), alpha = .2))
  + guides(alpha=FALSE)
  + ggtitle('Residual vs negemo')
  + ylab('Linear Residuals')
  + xlab('negemo')
  + theme_minimal()
  + theme(plot.title = element_text(hjust = 0.5),
          plot.title.position = "plot")
)

