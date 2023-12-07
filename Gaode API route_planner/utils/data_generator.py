import numpy as np
from pathlib import Path

# number of locations
n = 112

# matrix of random travel time between locations
# random_travel = np.random.randint(1, 100, (n, n))
# np.fill_diagonal(random_travel, 0)
# with open(f"{Path(__file__).parent}/../data/distance_matrix.csv", "wb") as f:
#     np.savetxt(f, random_travel.astype(int), fmt='%i', delimiter=",")

# random demand of retailers
random_demand = np.random.randint(1, 30, n)
random_demand[0] = 0  # demand of warehouse is 0
with open(f"{Path(__file__).parent}/../data/demands.csv", "wb") as f:
    np.savetxt(f, random_demand.astype(int), fmt='%i', delimiter=",")

# random carrier capacity
random_capacity = np.random.randint(25, 150, 22)
if (np.sum(random_demand) > np.sum(random_capacity)):
    random_capacity[0] += np.sum(random_demand) - np.sum(random_capacity)
with open(f"{Path(__file__).parent}/../data/vehicle_capacities.csv", "wb") as f:
    np.savetxt(f, random_capacity.astype(int), fmt='%i', delimiter=",")
