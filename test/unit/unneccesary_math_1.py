from .unneccesary_math_2 import multiply_and_add
from . import unneccesary_math_3


def divide(a, b):
    return a / b


def map_test(element):
    result = divide(element, 5)
    result = multiply_and_add(result, 5, 10)
    result = unneccesary_math_3.add(result, 5)
    return result
