def generate_combinations(json, combination = None):
  if not combination: combination = []
  combinations = []
  for element in json:
    if isinstance(element, str):
      # add the string to the current combination
      combination.append(element)
    elif isinstance(element, dict):
      # generate combinations for each key-value pair in the dictionary,
      # but not for the values themselves
      for key, value in element.items():
        new_combination = combination.copy()
        new_combination.append(key)
        combinations.extend(generate_combinations(value, new_combination))
    elif isinstance(element, list):
      # generate combinations for each element in the list
      combinations.extend(generate_combinations(element, combination))
  # return the current combination
  return combinations

data = [
  {"A1": [
      {"B1": ["C1", "C2", "C3"]},
      {"B2": ["C4", "C5"]},
      "B3"]},
  {"A2": ["B4", "B5"]},
  "A3",
]

combinations = generate_combinations(data)
print(combinations)