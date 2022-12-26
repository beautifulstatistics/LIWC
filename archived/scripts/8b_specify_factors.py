

with open("./data/dictionaries/linear_factors.txt",'r') as f:
    factors = set()
    for line in f:
        factors.add(line.split("/")[0].split("(")[0])
        
    predictor_cols = list(factors)

print(predictor_cols)