library(biglm)

print("Starting Preprocessing")
print(Sys.time())

hyps <- list(
  hyp1 = c('ppron','drives','relativ','percept'),
  hyp2 = c('cogproc','totallen'),
  hyp3 = c('posemo','negemo')
)

########################################

location <- "./data/counts/"
file_names <- paste0(location, dir(location))

file_names <- sample(file_names)

total_n <- 226838123
n <- length(file_names)
x <-  10
filen <- split(1:n, factor(sort(rank(1:n %% x))))

print("Starting first model.")
print(Sys.time())

files <- c(unlist(filen[1]), unlist(filen[2], unlist(filen[3])))
cou <- do.call(rbind, lapply(file_names[files], read.csv))

cou <- cou[,c(unlist(hyps),'permission_denied')]
cou$permission_denied <- cou$permission_denied == "True"

print("Starting first models.")
print(Sys.time())
print(dim(cou))

bigl <- list()
for (i in seq_along(hyps)) {
    form <- formula(paste0("permission_denied ~ ",paste0(i,collapse="*"),collapse = ""))
    bigl[[i]] <- bigglm(form,
                 data = cou, maxit = 10^6,
                 family = binomial())
    print(summary(bigl[[i]]))
}

nr <- nrow(cou)
print(nr)
print("First models fit.")
print(Sys.time())

print("Starting iterations.")

i <- 4
for (files in filen[4:x]) {
    cou <- do.call(rbind, lapply(file_names[files], read.csv))

    for (j in seq_along(bigl)) {
        bigl[[j]] <- update(bigl[[j]], cou)
    }

    nr_each <- nrow(cou)
    nr <- nr + nr_each
    print(nr)

    print(paste0(c(i, x), collapse = "/"))

    i <- i + 1
}

for (i in bigl) {
    print(summary(i))
}

print("Finished.")
print(Sys.time())