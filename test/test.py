import unittest

# TODO

with open("lots_of_data.csv") as fh:
    long_list = list(csv.reader(fh))

results = fastmap(big_function, long_list)

global_init("MY_TOKEN", confirm_charges=True)


if __name__ == '__main__':
    unittest.main()
