library(biglm)

print("Starting Preprocessing")
print(Sys.time())

form <- read.csv("./data/linear_model.formula", header = FALSE)
form <- form$V1

########################################

file_names <- paste0("./data/ready_for_models.csv/",
                     dir("./data/ready_for_models.csv/"))

file_names <- sample(file_names)

total_n <- 226838123
n <- length(file_names)
x <-  10
filen <- split(1:n, factor(sort(rank(1:n %% x))))

print("Starting first load.")
print(Sys.time())

files <- unlist(filen[1])
cou <- do.call(rbind, lapply(file_names[files], read.csv))

print("Starting first model.")
print(Sys.time())
print(dim(cou))
coum <- bigglm(formula(form), data = cou, maxit = 10^6, family = binomial())
save(file = "./data/regression.model", coum)
nr <- nrow(cou)
print(nr)
print("First model fit.")
print(Sys.time())

print("Starting iterations.")

i <- 2
for (files in filen[2:x]) {
    cou <- do.call(rbind, lapply(file_names[files], read.csv))
    coum <- update(coum, cou)
    nr_each <- nrow(cou)
    nr <- nr + nr_each
    print(nr)

    save(file = "./data/regression.model", coum)
    print(paste0(c(i, x), collapse = "/"))

    i <- i + 1
}

summary(coum)
print("Finished.")
print(Sys.time())