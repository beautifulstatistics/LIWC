library(biglm)

print("Starting Preprocessing")
print(Sys.time())

form <- read.csv("./data/single_models.formulas", header = FALSE)
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

files <- c(unlist(filen[1]), unlist(filen[2], unlist(filen[3])))
cou <- do.call(rbind, lapply(file_names[files], read.csv))

print("Starting first models.")
print(Sys.time())
print(dim(cou))

bigl <- list()
for (i in seq_along(form)) {
    strspl <- strsplit(form[i], "~")
    fact <- gsub(" ", "", strspl[[1]][2], fixed = TRUE)
    factt <- table(cou[, fact], cou[, "permission_denied"])

    if (sum(factt > 5) == 4) {
        bigl[[i]] <- bigglm(formula(form[i]),
                     data = cou, maxit = 10^6,
                     family = binomial())
        print(summary(bigl[[i]]))
    } else {
        print("Not Good")
        print(form[i])
    }

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