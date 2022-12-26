

with open("./data/dictionaries/linear_factors.txt",'r') as f:
    factors = set()
    for line in f:
        factors.add(line)
        
    predictor_cols = list(factors)